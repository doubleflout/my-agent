from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from agent.config_models import Config
from bootstrap.tools import CoreRuntime, build_core_runtime
from core.net.http import SharedHttpResources
from webapp.agent_executor import AgentExecutor, web_session_key
from webapp.proactive_scheduler import WebProactiveJob


_PROACTIVE_TICK_PROMPT = """[proactive tick]
你正在为当前用户的默认主动会话生成一条主动消息。
请结合长期记忆、最近上下文和主动消息规则判断现在是否应该发起对话。
如果现在不适合主动打扰，或者没有足够自然的话题，只返回 NO_PROACTIVE_CONTENT。
如果适合，只返回一条会直接展示给用户的中文消息，不要解释决策过程。
"""


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
    def __init__(self, runtime_manager: UserRuntimeManager) -> None:
        self.runtime_manager = runtime_manager

    async def run(self, job: WebProactiveJob) -> str | None:
        runtime = await self.runtime_manager.get_runtime(job.user_id)
        response = await runtime.loop.process_direct(
            content=_PROACTIVE_TICK_PROMPT,
            session_key=job.session_key,
            channel="web_proactive",
            chat_id=job.conversation_id,
            stream_events=False,
        )
        text = str(response or "").strip()
        if not text or text == "NO_PROACTIVE_CONTENT":
            return None
        return text
