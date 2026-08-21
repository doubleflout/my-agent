from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

QueueTopic = Literal[
    "agent.inbound",
    "agent.outbound",
    "agent.tasks",
    "agent.events",
]

QueueEventType = Literal[
    "user_message",
    "assistant_message",
    "proactive_push",
    "subagent_task",
    "subagent_result",
    "turn_event",
    "tool_event",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class QueueEnvelope:
    event_id: str
    topic: QueueTopic
    event_type: QueueEventType
    user_id: str
    session_key: str
    conversation_id: str = ""
    turn_id: str = ""
    source: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)

    @classmethod
    def new(
        cls,
        *,
        topic: QueueTopic,
        event_type: QueueEventType,
        user_id: str,
        session_key: str,
        conversation_id: str = "",
        turn_id: str = "",
        source: str = "",
        payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> QueueEnvelope:
        return cls(
            event_id=str(uuid4()),
            topic=topic,
            event_type=event_type,
            user_id=user_id,
            session_key=session_key,
            conversation_id=conversation_id,
            turn_id=turn_id,
            source=source,
            payload=payload or {},
            metadata=metadata or {},
        )
