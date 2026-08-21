from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random as _random_module
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable

from agent.config_models import Config
from agent.looping.ports import SessionServices
from agent.turns.orchestrator import TurnOrchestrator, TurnOrchestratorDeps
from agent.turns.outbound import OutboundDispatch, OutboundPort
from bus.message_queue import MessageQueueBackend
from bus.message_queue_types import QueueEnvelope
from bootstrap.tools import CoreRuntime, build_core_runtime
from core.net.http import SharedHttpResources
from proactive_v2.agent_tick_factory import AgentTickDeps, AgentTickFactory
from proactive_v2.anyaction import AnyActionGate, QuotaStore
from proactive_v2.judge import MessageDeduper
from proactive_v2.mcp_sources import McpClientPool
from proactive_v2.sensor import Sensor
from proactive_v2.state import build_proactive_state_store
from webapp.agent_executor import AgentExecutor, web_session_key
from webapp.proactive_scheduler import WebProactiveJob
from webapp.store import WebStore

logger = logging.getLogger(__name__)


class UserWorkspaceResolver:
    """Maps product users to isolated Akashic workspaces."""

    def __init__(self, base_workspace: Path) -> None:
        self.base_workspace = base_workspace
        self.users_root = base_workspace / "users"

    def for_user(self, user_id: str) -> Path:
        clean = str(user_id).strip()
        if not clean or any(part in clean for part in ("..", "/", "\\")):
            raise ValueError(f"invalid user_id for workspace: {user_id!r}")
        workspace = self.users_root / clean
        workspace.mkdir(parents=True, exist_ok=True)
        return workspace


@dataclass
class _RuntimeEntry:
    runtime: CoreRuntime
    last_used: float


class UserRuntimeManager:
    """Lazy per-user CoreRuntime cache for the Web API.

    This is an MVP isolation strategy: each active user gets an independent
    runtime bound to their own workspace. Idle runtimes are stopped and evicted.
    """

    def __init__(
        self,
        *,
        config: Config,
        base_workspace: Path,
        http_resources: SharedHttpResources,
        max_cached: int | None = None,
        ttl_seconds: int | None = None,
        runtime_builder: Callable[[Config, Path, SharedHttpResources], CoreRuntime] = build_core_runtime,
    ) -> None:
        self.config = config
        self.resolver = UserWorkspaceResolver(base_workspace)
        self.http_resources = http_resources
        self.runtime_builder = runtime_builder
        self.max_cached = max(1, int(max_cached or os.environ.get("AKASHIC_WEB_RUNTIME_CACHE_MAX", "20")))
        self.ttl_seconds = max(60, int(ttl_seconds or os.environ.get("AKASHIC_WEB_RUNTIME_TTL_SECONDS", "1800")))
        self._entries: dict[str, _RuntimeEntry] = {}
        self._lock = asyncio.Lock()

    def workspace_for_user(self, user_id: str) -> Path:
        return self.resolver.for_user(user_id)

    async def get_runtime(self, user_id: str) -> CoreRuntime:
        now = time.monotonic()
        async with self._lock:
            await self._evict_expired_locked(now)
            entry = self._entries.get(user_id)
            if entry is not None:
                entry.last_used = now
                return entry.runtime

            await self._evict_lru_if_needed_locked()
            workspace = self.workspace_for_user(user_id)
            runtime = self.runtime_builder(self.config, workspace, self.http_resources)
            await runtime.start()
            self._entries[user_id] = _RuntimeEntry(runtime=runtime, last_used=now)
            return runtime

    async def aclose(self) -> None:
        async with self._lock:
            entries = list(self._entries.values())
            self._entries.clear()
        for entry in entries:
            await entry.runtime.stop()

    async def _evict_expired_locked(self, now: float) -> None:
        expired = [
            user_id
            for user_id, entry in self._entries.items()
            if now - entry.last_used >= self.ttl_seconds
        ]
        for user_id in expired:
            entry = self._entries.pop(user_id)
            await entry.runtime.stop()

    async def _evict_lru_if_needed_locked(self) -> None:
        while len(self._entries) >= self.max_cached:
            user_id, entry = min(
                self._entries.items(),
                key=lambda item: item[1].last_used,
            )
            self._entries.pop(user_id)
            await entry.runtime.stop()


class UserRuntimeAgentExecutor(AgentExecutor):
    def __init__(self, runtime_manager: UserRuntimeManager) -> None:
        self.runtime_manager = runtime_manager

    async def run(
        self,
        *,
        content: str,
        user_id: str,
        conversation_id: str,
        session_key: str | None = None,
    ) -> str:
        runtime = await self.runtime_manager.get_runtime(user_id)
        return await runtime.loop.process_direct(
            content=content,
            session_key=session_key or web_session_key(user_id, conversation_id),
            channel="web",
            chat_id=conversation_id,
            stream_events=False,
        )


class UserRuntimeProactiveRunner:
    def __init__(
        self,
        runtime_manager: UserRuntimeManager,
        store: WebStore,
        message_queue: MessageQueueBackend | None = None,
    ) -> None:
        self.runtime_manager = runtime_manager
        self.store = store
        self.message_queue = message_queue

    async def run(self, job: WebProactiveJob) -> str | None:
        runtime = await self.runtime_manager.get_runtime(job.user_id)
        workspace = self.runtime_manager.workspace_for_user(job.user_id)
        proactive_cfg = replace(
            self.runtime_manager.config.proactive,
            default_channel="web_proactive",
            default_chat_id=job.conversation_id,
        )
        state_store = build_proactive_state_store(
            backend=getattr(self.runtime_manager.config.storage, "backend", "sqlite"),
            workspace_dir=workspace,
            database_url=self.runtime_manager.config.storage.postgres.database_url,
        )
        pool = McpClientPool(workspace)
        await pool.connect_all()
        try:
            sense = Sensor(
                cfg=proactive_cfg,
                sessions=runtime.session_manager,
                state=state_store,
                memory=runtime.memory_runtime.markdown.store,
                presence=runtime.presence,
                rng=_random_module.Random(),
                target_session_key=job.session_key,
            )
            anyaction = AnyActionGate(
                cfg=proactive_cfg,
                quota_store=QuotaStore(Path(state_store.workspace_dir) / "proactive_quota.json"),
                rng=_random_module.Random(),
            )
            deduper = (
                MessageDeduper(
                    provider=runtime.provider,
                    model=proactive_cfg.agent_tick_model or proactive_cfg.model or runtime.config.model,
                    max_tokens=1024,
                )
                if proactive_cfg.message_dedupe_enabled
                else None
            )
            orchestrator = TurnOrchestrator(
                TurnOrchestratorDeps(
                    session=SessionServices(
                        session_manager=runtime.session_manager,
                        presence=runtime.presence,
                    ),
                    outbound=_WebProactiveOutboundPort(
                        store=self.store,
                        user_id=job.user_id,
                        conversation_id=job.conversation_id,
                        session_key=job.session_key,
                        message_queue=self.message_queue,
                    ),
                )
            )
            tick = AgentTickFactory(
                AgentTickDeps(
                    cfg=proactive_cfg,
                    sense=sense,
                    presence=runtime.presence,
                    provider=runtime.provider,
                    model=proactive_cfg.model or runtime.config.model,
                    max_tokens=1024,
                    memory=runtime.memory_runtime,
                    state_store=state_store,
                    any_action_gate=anyaction,
                    passive_busy_fn=lambda _session_key: False,
                    deduper=deduper,
                    rng=_random_module.Random(),
                    workspace_context_fn=lambda: _read_web_proactive_context(workspace),
                    shared_tools=runtime.tools,
                    turn_orchestrator=orchestrator,
                    pool=pool,
                    tool_hooks=list(getattr(runtime.plugin_manager, "tool_hooks", []) or []),
                    target_session_key=job.session_key,
                    target_channel="web_proactive",
                    target_chat_id=job.conversation_id,
                )
            ).build()
            await tick.tick()
            return None
        finally:
            await pool.disconnect_all()
            close = getattr(state_store, "close", None)
            if callable(close):
                with contextlib.suppress(Exception):
                    close()


class _WebProactiveOutboundPort(OutboundPort):
    def __init__(
        self,
        *,
        store: WebStore,
        user_id: str,
        conversation_id: str,
        session_key: str,
        message_queue: MessageQueueBackend | None = None,
    ) -> None:
        self._store = store
        self._user_id = user_id
        self._conversation_id = conversation_id
        self._session_key = session_key
        self._message_queue = message_queue

    async def dispatch(self, outbound: OutboundDispatch) -> bool:
        content = str(outbound.content or "").strip()
        media = [str(item).strip() for item in outbound.media if str(item).strip()]
        if not content and not media:
            return False
        metadata: dict[str, Any] = {
            "source": "proactive",
            "session_key": self._session_key,
            **dict(outbound.metadata or {}),
        }
        if media:
            metadata["media"] = media
        self._store.add_message(
            conversation_id=self._conversation_id,
            user_id=self._user_id,
            role="assistant",
            content=content,
            metadata=metadata,
        )
        logger.info(
            "[web.proactive] persisted proactive message user=%s conversation=%s session=%s content_len=%d media=%d",
            self._user_id,
            self._conversation_id,
            self._session_key,
            len(content),
            len(media),
        )
        if self._message_queue is not None:
            await self._message_queue.publish(
                QueueEnvelope.new(
                    topic="agent.outbound",
                    event_type="proactive_push",
                    user_id=self._user_id,
                    session_key=self._session_key,
                    conversation_id=self._conversation_id,
                    source="proactive",
                    payload={
                        "role": "assistant",
                        "content": content,
                        "media": media,
                    },
                    metadata=metadata,
                )
            )
        return True


def _read_web_proactive_context(workspace: Path) -> str:
    path = workspace / "PROACTIVE_CONTEXT.md"
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""
