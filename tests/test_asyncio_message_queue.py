import pytest

from bus.backends.asyncio_message_queue import AsyncioMessageQueue
from bus.message_queue_types import QueueEnvelope


@pytest.mark.asyncio
async def test_asyncio_message_queue_publish_and_subscribe() -> None:
    queue = AsyncioMessageQueue()
    msg = QueueEnvelope.new(
        topic="agent.outbound",
        event_type="assistant_message",
        user_id="u1",
        session_key="web:u1:c1",
        payload={"content": "ok"},
    )

    await queue.publish(msg)
    stream = queue.subscribe("agent.outbound", consumer_name="test")
    received = await anext(stream)

    assert received.event_id == msg.event_id
    await queue.ack(received)
    await queue.close()


@pytest.mark.asyncio
async def test_asyncio_message_queue_nack_requeues_when_retry_true() -> None:
    queue = AsyncioMessageQueue()
    msg = QueueEnvelope.new(
        topic="agent.tasks",
        event_type="subagent_task",
        user_id="u1",
        session_key="web:u1:c1",
    )

    await queue.publish(msg)
    stream = queue.subscribe("agent.tasks", consumer_name="worker")
    first = await anext(stream)
    await queue.nack(first, retry=True, reason="temporary")
    second = await anext(stream)

    assert second.event_id == msg.event_id
    await queue.close()
