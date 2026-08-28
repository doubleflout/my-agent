from __future__ import annotations

import pytest

from agent.tools.base import Tool
from agent.tools.registry import ToolRegistry


class ContextEchoTool(Tool):
    name = "context_echo"
    description = "Echo hidden tool context"
    parameters = {"type": "object", "properties": {}}

    async def execute(self, **kwargs):
        return str(kwargs.get("session_key") or "")


@pytest.mark.asyncio
async def test_tool_registry_injects_session_key_context():
    registry = ToolRegistry()
    registry.register(ContextEchoTool())
    registry.set_context(
        session_key="web:user-1:conversation-1",
        channel="web",
        chat_id="conversation-1",
    )

    result = await registry.execute("context_echo", {})

    assert result == "web:user-1:conversation-1"
