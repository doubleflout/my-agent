from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from agent.turns.outbound import OutboundDispatch
from agent.config_models import Config
from webapp.store import WebStore
from webapp.runtime_manager import (
    UserRuntimeAgentExecutor,
    UserRuntimeManager,
    UserWorkspaceResolver,
    _WebProactiveOutboundPort,
)


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


class FakeWebStore:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    def add_message(self, **kwargs) -> None:
        self.messages.append(kwargs)


class FakeMessageQueue:
    def __init__(self) -> None:
        self.messages: list[object] = []

    async def publish(self, message) -> None:
        self.messages.append(message)


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
    assert workspace == tmp_path / "workspace" / "users" / "user-1"
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
        assert built[0].workspace == tmp_path / "workspace" / "users" / "u1"
        await manager.aclose()

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_web_proactive_outbound_port_publishes_proactive_push_queue_event() -> None:
    store = FakeWebStore()
    queue = FakeMessageQueue()
    port = _WebProactiveOutboundPort(
        store=store,  # type: ignore[arg-type]
        user_id="user-1",
        conversation_id="conversation-1",
        session_key="web:proactive:user-1:conversation-1",
        message_queue=queue,  # type: ignore[arg-type]
    )

    dispatched = await port.dispatch(
        OutboundDispatch(
            channel="web_proactive",
            chat_id="conversation-1",
            content="该休息一下了",
            media=["image.png"],
            metadata={"source_id": "fitness"},
        )
    )

    assert dispatched is True
    assert store.messages[0]["content"] == "该休息一下了"
    assert len(queue.messages) == 1
    event = queue.messages[0]
    assert event.topic == "agent.outbound"
    assert event.event_type == "proactive_push"
    assert event.user_id == "user-1"
    assert event.session_key == "web:proactive:user-1:conversation-1"
    assert event.conversation_id == "conversation-1"
    assert event.source == "proactive"
    assert event.payload == {
        "role": "assistant",
        "content": "该休息一下了",
        "media": ["image.png"],
    }
    assert event.metadata["session_key"] == "web:proactive:user-1:conversation-1"
    assert event.metadata["source_id"] == "fitness"


@pytest.mark.asyncio
async def test_web_proactive_outbound_port_persists_message_to_proactive_session(tmp_path) -> None:
    store = WebStore("sqlite:///" + (tmp_path / "webapp.db").as_posix())
    try:
        user = store.create_user(
            email="u1@example.com",
            password_hash="hash",
            display_name=None,
        )
        conversation = store.ensure_default_proactive_session(user_id=user.id)
        port = _WebProactiveOutboundPort(
            store=store,
            user_id=user.id,
            conversation_id=conversation.id,
            session_key=conversation.session_key,
        )

        dispatched = await port.dispatch(
            OutboundDispatch(
                channel="web_proactive",
                chat_id=conversation.id,
                content="主动消息落库验证",
            )
        )

        rows = store.list_messages(user_id=user.id, conversation_id=conversation.id)
        assert dispatched is True
        assert len(rows) == 1
        assert rows[0].role == "assistant"
        assert rows[0].content == "主动消息落库验证"
        assert rows[0].metadata["source"] == "proactive"
        assert rows[0].metadata["session_key"] == conversation.session_key
        assert rows[0].id.startswith(conversation.session_key + ":")
    finally:
        store.close()
