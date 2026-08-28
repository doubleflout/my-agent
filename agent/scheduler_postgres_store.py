from __future__ import annotations

import json
import threading
from dataclasses import asdict
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


def _dsn(database_url: str) -> str:
    return database_url.replace("postgresql+psycopg://", "postgresql://", 1)


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def _job_spec(job: Any) -> dict[str, Any]:
    data = asdict(job)
    for key in ("fire_at", "created_at"):
        value = data.get(key)
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


class PostgresScheduleStore:
    """PostgreSQL mirror for SchedulerService jobs.

    The JSON JobStore remains the runtime source of truth for the current
    scheduler. This store mirrors additions into public.schedules so Web/API
    layers can query user-owned scheduled jobs.
    """

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._lock = threading.RLock()
        self._conn = psycopg.connect(_dsn(database_url), row_factory=dict_row)
        self._ensure_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _ensure_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schedules (
                    id UUID PRIMARY KEY,
                    user_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
                    session_key TEXT NOT NULL REFERENCES sessions(key) ON DELETE CASCADE,
                    name TEXT NOT NULL,
                    spec_json JSONB NOT NULL,
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_schedules_session_enabled
                ON schedules(session_key, enabled)
                """
            )
            self._conn.commit()

    def _lookup_user_id(self, session_key: str) -> str | None:
        row = self._conn.execute(
            "SELECT user_id FROM sessions WHERE key = %s",
            (session_key,),
        ).fetchone()
        if row and row.get("user_id"):
            return str(row["user_id"])
        return None

    def upsert_job(
        self,
        *,
        job: Any,
        session_key: str,
        user_id: str | None = None,
    ) -> None:
        clean_session_key = str(session_key or "").strip()
        if not clean_session_key:
            return
        with self._lock:
            spec = _job_spec(job)
            resolved_user_id = str(user_id or "").strip() or self._lookup_user_id(clean_session_key)
            if not resolved_user_id:
                resolved_user_id = None
            name = str(getattr(job, "name", "") or "").strip() or str(getattr(job, "id"))[:8]
            self._conn.execute(
                """
                INSERT INTO schedules(id, user_id, session_key, name, spec_json, enabled, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(id) DO UPDATE SET
                    user_id = COALESCE(excluded.user_id, schedules.user_id),
                    session_key = excluded.session_key,
                    name = excluded.name,
                    spec_json = excluded.spec_json,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    str(getattr(job, "id")),
                    resolved_user_id,
                    clean_session_key,
                    name,
                    Jsonb(json.loads(json.dumps(spec, ensure_ascii=False, default=str))),
                    bool(getattr(job, "enabled", True)),
                    spec.get("created_at") or _now_iso(),
                    _now_iso(),
                ),
            )
            self._conn.commit()

    def disable_job(self, job_id: str) -> None:
        clean = str(job_id or "").strip()
        if not clean:
            return
        with self._lock:
            self._conn.execute(
                """
                UPDATE schedules
                SET enabled = FALSE,
                    updated_at = %s
                WHERE id = %s
                """,
                (_now_iso(), clean),
            )
            self._conn.commit()
