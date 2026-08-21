from session.postgres_store import _split_session_key, _web_user_id_from_session_key


def test_web_user_id_from_regular_web_session_key() -> None:
    assert _web_user_id_from_session_key("web:user-1:conversation-1") == "user-1"


def test_web_user_id_from_proactive_web_session_key() -> None:
    assert (
        _web_user_id_from_session_key("web:proactive:user-1:conversation-1")
        == "user-1"
    )


def test_split_regular_web_session_key() -> None:
    assert _split_session_key("web:user-1:conversation-1") == (
        "web",
        "conversation-1",
    )


def test_split_proactive_web_session_key() -> None:
    assert _split_session_key("web:proactive:user-1:conversation-1") == (
        "web_proactive",
        "conversation-1",
    )
