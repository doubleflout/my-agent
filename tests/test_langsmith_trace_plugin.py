from __future__ import annotations

import shutil
import sys
import types
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from agent.config_models import Config, EvalConfig, LangSmithEvalConfig
from agent.plugins.manager import PluginManager
from bus.event_bus import EventBus
from bus.events_lifecycle import PhaseCompleted, TurnCommitted, TurnStarted


def _config() -> Config:
    return Config(
        provider="test",
        model="test-model",
        api_key="",
        system_prompt="test",
        eval=EvalConfig(
            langsmith=LangSmithEvalConfig(
                enabled=True,
                project="turn-trace-test",
                api_key="test-key",
                endpoint="https://api.smith.langchain.com",
            )
        ),
    )


class _FakeClient:
    instances: list["_FakeClient"] = []

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.flushed = False
        self.closed = False
        self.instances.append(self)

    def flush(self) -> None:
        self.flushed = True

    def close(self) -> None:
        self.closed = True


class _FakeRunTree:
    roots: list["_FakeRunTree"] = []

    def __init__(self, **kwargs: Any) -> None:
        self.name = str(kwargs["name"])
        self.run_type = str(kwargs.get("run_type", "chain"))
        self.inputs = dict(kwargs.get("inputs") or {})
        self.extra = dict(kwargs.get("extra") or {})
        self.project_name = str(kwargs.get("project_name") or "")
        self.client = kwargs.get("ls_client")
        self.parent: _FakeRunTree | None = kwargs.get("parent_run")
        self.children: list[_FakeRunTree] = []
        self.outputs: dict[str, object] | None = None
        self.error: str | None = None
        self.posted = False
        self.patched = False
        if self.parent is None:
            self.roots.append(self)

    def create_child(self, **kwargs: Any) -> "_FakeRunTree":
        child = _FakeRunTree(parent_run=self, **kwargs)
        self.children.append(child)
        return child

    def post(self) -> None:
        self.posted = True

    def end(
        self,
        *,
        outputs: dict[str, object] | None = None,
        error: str | None = None,
        **kwargs: object,
    ) -> None:
        self.outputs = outputs
        self.error = error

    def patch(self, **kwargs: object) -> None:
        self.patched = True


def _install_fake_langsmith(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeClient.instances.clear()
    _FakeRunTree.roots.clear()
    fake_langsmith = types.ModuleType("langsmith")
    fake_langsmith.Client = _FakeClient
    fake_run_trees = types.ModuleType("langsmith.run_trees")
    fake_run_trees.RunTree = _FakeRunTree
    monkeypatch.setitem(sys.modules, "langsmith", fake_langsmith)
    monkeypatch.setitem(sys.modules, "langsmith.run_trees", fake_run_trees)


async def _load_plugin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[EventBus, PluginManager]:
    _install_fake_langsmith(monkeypatch)
    source = Path(__file__).parents[1] / "plugins" / "langsmith_trace"
    plugin_root = tmp_path / "plugins"
    shutil.copytree(source, plugin_root / "langsmith_trace")
    bus = EventBus()
    manager = PluginManager(
        plugin_dirs=[plugin_root],
        event_bus=bus,
        workspace=tmp_path,
        app_config=_config(),
    )
    await manager.load_all()
    return bus, manager


def _started(turn_id: str, chat_id: str = "conversation-1") -> TurnStarted:
    return TurnStarted(
        turn_id=turn_id,
        session_key=f"web:user-1:{chat_id}",
        channel="web",
        chat_id=chat_id,
        content="你好",
        timestamp=datetime(2026, 9, 5, 9, 30),
    )


def _phase(turn_id: str, phase: str, chat_id: str = "conversation-1") -> PhaseCompleted:
    return PhaseCompleted(
        turn_id=turn_id,
        phase=phase,
        session_key=f"web:user-1:{chat_id}",
        channel="web",
        chat_id=chat_id,
        input_summary={"message_chars": 2},
        output_summary={"skill_count": 1},
    )


def _committed(turn_id: str, chat_id: str = "conversation-1") -> TurnCommitted:
    return TurnCommitted(
        turn_id=turn_id,
        session_key=f"web:user-1:{chat_id}",
        channel="web",
        chat_id=chat_id,
        input_message="你好",
        persisted_user_message="你好",
        assistant_response="收到",
        tools_used=["read_file"],
        raw_reply="收到",
        tool_chain_raw=[{"text": "", "calls": [{"name": "read_file"}]}],
        post_reply_budget={"prompt_tokens": 10},
        react_stats={"iteration_count": 1},
    )


@pytest.mark.asyncio
async def test_langsmith_plugin_builds_one_trace_tree_per_turn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus, manager = await _load_plugin(tmp_path, monkeypatch)

    await bus.fanout(_started("turn-1"))
    await bus.fanout(_phase("turn-1", "before_turn"))
    await bus.fanout(_committed("turn-1"))
    assert _FakeRunTree.roots[0].patched is False
    await bus.fanout(_phase("turn-1", "after_turn"))

    assert len(_FakeRunTree.roots) == 1
    root = _FakeRunTree.roots[0]
    assert root.name == "agent_turn"
    assert root.posted is True
    assert root.patched is True
    assert root.extra["metadata"]["turn_id"] == "turn-1"
    assert root.outputs == {
        "assistant_response": "收到",
        "raw_reply": "收到",
        "tools_used": ["read_file"],
        "tool_chain": [{"text": "", "calls": [{"name": "read_file"}]}],
        "post_reply_budget": {"prompt_tokens": 10},
        "react_stats": {"iteration_count": 1},
        "error": None,
    }
    assert [child.name for child in root.children] == [
        "phase.before_turn",
        "phase.after_turn",
    ]
    assert all(child.posted and child.patched for child in root.children)
    assert root.children[0].inputs == {"message_chars": 2}
    assert root.children[0].outputs == {"skill_count": 1}

    await manager.terminate_all()
    await bus.aclose()


@pytest.mark.asyncio
async def test_langsmith_plugin_isolates_concurrent_turn_trees(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus, manager = await _load_plugin(tmp_path, monkeypatch)

    await bus.fanout(_started("turn-1", "conversation-1"))
    await bus.fanout(_started("turn-2", "conversation-2"))
    await bus.fanout(_phase("turn-2", "before_turn", "conversation-2"))
    await bus.fanout(_phase("turn-1", "before_turn", "conversation-1"))
    await bus.fanout(_committed("turn-1", "conversation-1"))
    await bus.fanout(_committed("turn-2", "conversation-2"))
    await bus.fanout(_phase("turn-2", "after_turn", "conversation-2"))
    await bus.fanout(_phase("turn-1", "after_turn", "conversation-1"))

    roots = {
        str(root.extra["metadata"]["turn_id"]): root
        for root in _FakeRunTree.roots
    }
    assert set(roots) == {"turn-1", "turn-2"}
    assert roots["turn-1"].children[0].extra["metadata"]["turn_id"] == "turn-1"
    assert roots["turn-2"].children[0].extra["metadata"]["turn_id"] == "turn-2"
    assert roots["turn-1"].patched is True
    assert roots["turn-2"].patched is True

    await manager.terminate_all()
    await bus.aclose()


@pytest.mark.asyncio
async def test_langsmith_plugin_closes_unfinished_trace_on_shutdown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bus, manager = await _load_plugin(tmp_path, monkeypatch)
    await bus.fanout(_started("turn-1"))

    await manager.terminate_all()
    await bus.aclose()

    root = _FakeRunTree.roots[0]
    assert root.error == "trace interrupted during plugin shutdown"
    assert root.patched is True
    assert _FakeClient.instances[0].flushed is True
    assert _FakeClient.instances[0].closed is True
