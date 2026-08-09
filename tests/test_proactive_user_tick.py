from __future__ import annotations

from datetime import datetime, timedelta, timezone

from proactive_v2.config import ProactiveConfig
from proactive_v2.user_tick import (
    compute_user_tick_interval,
    user_id_from_web_session_key,
)


def test_user_id_from_web_proactive_session_key() -> None:
    assert (
        user_id_from_web_session_key("web:proactive:user-1:conversation-1")
        == "user-1"
    )


def test_user_tick_interval_uses_most_recent_session_activity() -> None:
    cfg = ProactiveConfig(
        tick_jitter=0,
        tick_interval_s0=4800,
        tick_interval_s1=2400,
        tick_interval_s2=1080,
        tick_interval_s3=420,
    )
    now = datetime(2026, 8, 9, 8, 0, tzinfo=timezone.utc)

    interval = compute_user_tick_interval(
        cfg=cfg,
        user_last_user_at=[
            now - timedelta(days=2),
            now - timedelta(minutes=1),
        ],
        now=now,
    )

    assert interval == 4800


def test_user_tick_interval_gets_shorter_when_user_has_been_idle() -> None:
    cfg = ProactiveConfig(
        tick_jitter=0,
        tick_interval_s0=4800,
        tick_interval_s1=2400,
        tick_interval_s2=1080,
        tick_interval_s3=420,
    )
    now = datetime(2026, 8, 9, 8, 0, tzinfo=timezone.utc)

    interval = compute_user_tick_interval(
        cfg=cfg,
        user_last_user_at=[now - timedelta(days=2)],
        now=now,
    )

    assert interval == 2400
