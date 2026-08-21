from bus.message_queue_types import QueueEnvelope


def test_queue_envelope_new_sets_required_fields() -> None:
    msg = QueueEnvelope.new(
        topic="agent.outbound",
        event_type="proactive_push",
        user_id="u1",
        session_key="web:proactive:u1:c1",
        conversation_id="c1",
        turn_id="t1",
        source="proactive",
        payload={"content": "hello"},
        metadata={"priority": "normal"},
    )

    assert msg.event_id
    assert msg.topic == "agent.outbound"
    assert msg.event_type == "proactive_push"
    assert msg.user_id == "u1"
    assert msg.session_key == "web:proactive:u1:c1"
    assert msg.conversation_id == "c1"
    assert msg.turn_id == "t1"
    assert msg.source == "proactive"
    assert msg.payload == {"content": "hello"}
    assert msg.metadata == {"priority": "normal"}
    assert msg.created_at.tzinfo is not None
