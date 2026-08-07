from __future__ import annotations

import asyncio
from pathlib import Path

from agent.config_models import Config
from webapp.runtime_manager import UserRuntimeAgentExecutor, UserRuntimeManager, UserWorkspaceResolver


class FakeLoop:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    async def process_direct(
        self,
        *,
        content: str,
        session_key: str,
        channel: str,
        chat_id: str,
        stream_events: bool,
    ) -> str:
        self.calls.append(
            {
                "content": content,
                "session_key": session_key,
                "channel": channel,
                "chat_id": chat_id,
                "stream_events": str(stream_events),
            }
        )
        return "ok"


class FakeRuntime:
    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.loop = FakeLoop()
        self.started = 0
        self.stopped = 0

    async def start(self) -> None:
        self.started += 1

    async def stop(self) -> None:
        self.stopped += 1


class FakeHttpResources:
    pass


def make_config() -> Config:
    return Config(
        provider="openai",
        model="test",
        api_key="",
        system_prompt="test",
    )


def test_workspace_resolver_uses_users_root_for_default_workspace(tmp_path):
    resolver = UserWorkspaceResolver(tmp_path / "workspace")
    workspace = resolver.for_user("user-1")
    assert workspace == tmp_path / "users" / "user-1"
    assert workspace.exists()


def test_runtime_manager_reuses_runtime_per_user_and_evicts_lru(tmp_path):
    async def scenario() -> None:
        built: list[FakeRuntime] = []

        def builder(config, workspace, http_resources):
            runtime = FakeRuntime(workspace)
            built.append(runtime)
            return runtime

        manager = UserRuntimeManager(
            config=make_config(),
            base_workspace=tmp_path / "workspace",
            http_resources=FakeHttpResources(),  # type: ignore[arg-type]
            max_cached=1,
            ttl_seconds=60,
            runtime_builder=builder,  # type: ignore[arg-type]
        )
        first = await manager.get_runtime("u1")
        again = await manager.get_runtime("u1")
        second = await manager.get_runtime("u2")

        assert first is again
        assert second is not first
        assert built[0].stopped == 1
        assert built[1].started == 1
        await manager.aclose()
        assert built[1].stopped == 1

    asyncio.run(scenario())


def test_user_runtime_agent_executor_uses_web_session_key(tmp_path):
    async def scenario() -> None:
        built: list[FakeRuntime] = []

        def builder(config, workspace, http_resources):
            runtime = FakeRuntime(workspace)
            built.append(runtime)
            return runtime

        manager = UserRuntimeManager(
            config=make_config(),
            base_workspace=tmp_path / "workspace",
            http_resources=FakeHttpResources(),  # type: ignore[arg-type]
            runtime_builder=builder,  # type: ignore[arg-type]
        )
        executor = UserRuntimeAgentExecutor(manager)
        result = await executor.run(content="hello", user_id="u1", conversation_id="c1")

        assert result == "ok"
        assert built[0].loop.calls[0]["session_key"] == "web:u1:c1"
        assert built[0].loop.calls[0]["channel"] == "web"
        assert built[0].workspace == tmp_path / "users" / "u1"
        await manager.aclose()

    asyncio.run(scenario())
