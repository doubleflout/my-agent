from __future__ import annotations

import random as _random_module
from datetime import datetime, timezone
from typing import Iterable, Any

from proactive_v2.energy import compute_energy, d_energy, next_tick_from_score


def user_id_from_web_session_key(session_key: str) -> str | None:
    parts = str(session_key or "").split(":")
    if len(parts) >= 3 and parts[0] == "web" and parts[1] != "proactive":
        return parts[1] or None
    if len(parts) >= 4 and parts[0] == "web" and parts[1] == "proactive":
        return parts[2] or None
    return None


def most_recent_datetime(values: Iterable[datetime | None]) -> datetime | None:
    clean = [_as_aware_utc(value) for value in values if value is not None]
    if not clean:
        return None
    return max(clean)


def _as_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def compute_user_tick_interval(
    *,
    cfg: Any,
    user_last_user_at: Iterable[datetime | None],
    now: datetime,
    rng: _random_module.Random | None = None,
) -> int:
    tick_now = _as_aware_utc(now)
    last_user_at = most_recent_datetime(user_last_user_at)
    energy = compute_energy(last_user_at, tick_now)
    base_score = d_energy(energy) * cfg.score_weight_energy
    return next_tick_from_score(
        base_score,
        tick_s3=cfg.tick_interval_s3,
        tick_s2=cfg.tick_interval_s2,
        tick_s1=cfg.tick_interval_s1,
        tick_s0=cfg.tick_interval_s0,
        tick_jitter=cfg.tick_jitter,
        rng=rng,
    )
