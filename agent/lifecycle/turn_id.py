from __future__ import annotations

from datetime import datetime
from typing import Any


def resolve_passive_turn_id(
    *,
    session_key: str,
    timestamp: datetime,
    metadata: dict[str, Any] | None = None,
) -> str:
    supplied = str((metadata or {}).get("turn_id") or "").strip()
    if supplied:
        return supplied
    return f"passive:{session_key}:{timestamp.isoformat()}"
