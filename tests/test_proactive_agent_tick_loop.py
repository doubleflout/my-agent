from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from agent.tool_hooks.base import ToolHook
from agent.tool_hooks.types import HookContext, HookOutcome
from proactive_v2.agent_tick import AgentTick
from proactive_v2.config import ProactiveConfig
from proactive_v2.context import AgentTickContext
from proactive_v2.gateway import GatewayDeps, GatewayResult
from proactive_v2.tools import ToolDeps


class DenyFinishTurnHook(ToolHook):
    @property
    def name(self) -> str:
        return "deny_finish_turn"

    @property
    def event(self) -> str:
        return "pre_tool_use"

    def matches(self, ctx: HookContext) -> bool:
        return ctx.request.tool_name == "finish_turn"

    async def run(self, ctx: HookContext) -> HookOutcome:
        return HookOutcome(decision="deny", reason="blocked")


@pytest.mark.asyncio
async def test_agent_tick_commits_existing_draft_when_finish_turn_is_denied() -> None:
    calls = [
        {"id": "call_1", "name": "message_push", "input": {"message": "hello", "evidence": []}},
        {"id": "call_2", "name": "finish_turn", "input": {"decision": "reply"}},
    ]

    async def llm_fn(messages, schemas, tool_choice):
        del messages, schemas, tool_choice
        return calls.pop(0) if calls else None

    async def alert_fn() -> list[dict[str, Any]]:
        return [
            {
                "id": "a1",
                "event_id": "a1",
                "ack_server": "test-alert",
                "title": "test",
            }
        ]

    async def empty_fn(*args, **kwargs) -> list[dict[str, Any]]:
        del args, kwargs
        return []

    tick = AgentTick(
        cfg=ProactiveConfig(default_chat_id="conversation-1", agent_tick_max_steps=4),
        session_key="web:proactive:u1:conversation-1",
        state_store=MagicMock(),
        any_action_gate=None,
        last_user_at_fn=lambda: None,
        passive_busy_fn=None,
        deduper=None,
        tool_deps=ToolDeps(recent_chat_fn=lambda n=20: []),
        gateway_deps=GatewayDeps(
            alert_fn=alert_fn,
            feed_fn=empty_fn,
            context_fn=empty_fn,
        ),
        llm_fn=llm_fn,
        tool_hooks=[DenyFinishTurnHook()],
    )
    ctx = AgentTickContext(session_key="web:proactive:u1:conversation-1")
    entered = await tick._run_loop(ctx)

    assert entered is True
    assert ctx.terminal_action == "reply"
    assert ctx.final_message == "hello"
