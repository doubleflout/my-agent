from bus.message_queue import MessageQueueBackend


def test_message_queue_backend_protocol_importable() -> None:
    assert MessageQueueBackend is not None
