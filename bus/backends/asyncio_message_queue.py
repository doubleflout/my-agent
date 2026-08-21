from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from bus.message_queue_types import QueueEnvelope, QueueTopic


class AsyncioMessageQueue:
    """In-process message queue backend for local development and MVP wiring."""

    def __init__(self) -> None:
        self._queues: dict[QueueTopic, asyncio.Queue[QueueEnvelope]] = {}
        self._closed = False

    async def publish(self, message: QueueEnvelope) -> None:
        if self._closed:
            raise RuntimeError("message queue is closed")
        await self._queue(message.topic).put(message)

    async def subscribe(
        self,
        topic: QueueTopic,
        *,
        consumer_name: str,
    ) -> AsyncIterator[QueueEnvelope]:
        del consumer_name
        queue = self._queue(topic)
        while not self._closed:
            yield await queue.get()

    async def ack(self, message: QueueEnvelope) -> None:
        del message

    async def nack(
        self,
        message: QueueEnvelope,
        *,
        retry: bool = True,
        reason: str = "",
    ) -> None:
        del reason
        if retry:
            await self.publish(message)

    async def close(self) -> None:
        self._closed = True

    def _queue(self, topic: QueueTopic) -> asyncio.Queue[QueueEnvelope]:
        queue = self._queues.get(topic)
        if queue is None:
            queue = asyncio.Queue()
            self._queues[topic] = queue
        return queue
