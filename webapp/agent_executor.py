from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

StreamEventHandler = Callable[[dict[str, str]], Awaitable[None]]


def web_session_key(user_id: str, conversation_id: str) -> str:
    return f"web:{user_id}:{conversation_id}"


class AgentExecutor(Protocol):
    async def run(
        self,
        *,
        content: str,
        user_id: str,
        conversation_id: str,
        session_key: str | None = None,
        on_stream_event: StreamEventHandler | None = None,
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
        session_key: str | None = None,
        on_stream_event: StreamEventHandler | None = None,
    ) -> str:
        actual_session_key = session_key or web_session_key(user_id, conversation_id)
        if on_stream_event is not None:
            self._agent_loop.set_stream_sink_factory(
                lambda msg: on_stream_event
                if str(getattr(msg, "session_key", "")) == actual_session_key
                else None
            )
        return await self._agent_loop.process_direct(
            content=content,
            session_key=actual_session_key,
            channel="web",
            chat_id=conversation_id,
            stream_events=on_stream_event is not None,
        )
