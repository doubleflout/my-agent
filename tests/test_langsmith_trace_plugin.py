from __future__ import annotations

import shutil
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent.config_models import Config, EvalConfig, LangSmithEvalConfig
from agent.plugins.manager import PluginManager
from bus.event_bus import EventBus
from bus.events_lifecycle import TurnCommitted


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


@pytest.mark.asyncio
async def test_langsmith_trace_plugin_records_turn_committed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    calls: list[dict[str, object]] = []

    fake_langsmith = types.ModuleType("langsmith")

    def fake_traceable(*, name: str, run_type: str, metadata: dict[str, object] | None = None):
        def decorate(fn):
            def wrapper(payload):
                outputs = fn(payload)
                calls.append(
                    {
                        "name": name,
                        "run_type": run_type,
                        "metadata": metadata,
                        "inputs": payload,
                        "outputs": outputs,
                    }
                )
                return outputs

            return wrapper

        return decorate

    class FakeTracingContext:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def __enter__(self) -> None:
            return None

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def fake_tracing_context(**kwargs: object) -> FakeTracingContext:
        return FakeTracingContext(**kwargs)

    fake_langsmith.traceable = fake_traceable
    fake_langsmith.tracing_context = fake_tracing_context
    monkeypatch.setitem(sys.modules, "langsmith", fake_langsmith)

    source = Path(__file__).parents[1] / "plugins" / "langsmith_trace"
    plugin_root = tmp_path / "plugins"
    shutil.copytree(source, plugin_root / "langsmith_trace")

    bus = EventBus()
    mgr = PluginManager(
        plugin_dirs=[plugin_root],
        event_bus=bus,
        workspace=tmp_path,
        app_config=_config(),
    )

    await mgr.load_all()
    await bus.fanout(
        TurnCommitted(
            session_key="web:user-1:conversation-1",
            channel="web",
            chat_id="conversation-1",
            input_message="你好",
            persisted_user_message="你好",
            assistant_response="收到",
            tools_used=["read_file"],
            raw_reply="收到",
            tool_chain_raw=[{"text": "", "calls": [{"name": "read_file"}]}],
            post_reply_budget={"prompt_tokens": 10},
            react_stats={"iteration_count": 1},
            extra={"turn_id": "turn-1"},
        )
    )

    await mgr.terminate_all()
    await bus.aclose()

    assert len(calls) == 1
    call = calls[0]
    assert call["name"] == "agent_turn"
    assert call["run_type"] == "chain"
    assert call["metadata"] == {
        "session_key": "web:user-1:conversation-1",
        "channel": "web",
        "chat_id": "conversation-1",
        "turn_id": "turn-1",
    }
    assert call["inputs"] == {
        "session_key": "web:user-1:conversation-1",
        "channel": "web",
        "chat_id": "conversation-1",
        "message": "你好",
        "persisted_user_message": "你好",
    }
    assert call["outputs"] == {
        "assistant_response": "收到",
        "raw_reply": "收到",
        "tools_used": ["read_file"],
        "tool_chain": [{"text": "", "calls": [{"name": "read_file"}]}],
        "post_reply_budget": {"prompt_tokens": 10},
        "react_stats": {"iteration_count": 1},
        "error": None,
    }
