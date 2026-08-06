from __future__ import annotations

from typing import Protocol


def web_session_key(user_id: str, conversation_id: str) -> str:
    return f"web:{user_id}:{conversation_id}"


class AgentExecutor(Protocol):
    async def run(
        self,
        *,
        content: str,
        user_id: str,
        conversation_id: str,
    ) -> str:
        ...


class AgentLoopExecutor:
    def __init__(self, agent_loop) -> None:
        self._agent_loop = agent_loop

    async def run(
        self,
        *,
        content: str,
        user_id: str,
        conversation_id: str,
    ) -> str:
        return await self._agent_loop.process_direct(
            content=content,
            session_key=web_session_key(user_id, conversation_id),
            channel="web",
            chat_id=conversation_id,
            stream_events=False,
        )

