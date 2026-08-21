from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from bus.message_queue_types import QueueEnvelope, QueueTopic


class MessageQueueBackend(Protocol):
    async def publish(self, message: QueueEnvelope) -> None: ...

    async def subscribe(
        self,
        topic: QueueTopic,
        *,
        consumer_name: str,
    ) -> AsyncIterator[QueueEnvelope]: ...

    async def ack(self, message: QueueEnvelope) -> None: ...

    async def nack(
        self,
        message: QueueEnvelope,
        *,
        retry: bool = True,
        reason: str = "",
    ) -> None: ...

    async def close(self) -> None: ...
