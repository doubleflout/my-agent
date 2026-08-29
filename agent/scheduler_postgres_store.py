from __future__ import annotations

import json
import threading
from dataclasses import asdict
from datetime import datetime, timezone
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
    """PostgreSQL-backed SchedulerService jobs.

    When attached to SchedulerService this store is the runtime source of truth;
    the JSON JobStore remains a local compatibility mirror.
    """

    runtime_source = True

    def __init__(self, database_url: str, *, user_id: str | None = None) -> None:
        self.database_url = database_url
        self.user_id = str(user_id or "").strip() or None
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

    @staticmethod
    def _parse_dt(value: Any) -> datetime:
        if isinstance(value, datetime):
            dt = value
        else:
            text = str(value or "").strip()
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            dt = datetime.fromisoformat(text) if text else datetime.now(timezone.utc)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def load_jobs(self) -> list[Any]:
        from agent.scheduler import ScheduledJob

        where = ["enabled = TRUE"]
        params: list[object] = []
        if self.user_id:
            where.append("user_id = %s")
            params.append(self.user_id)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT id, user_id, session_key, name, spec_json, enabled, created_at
                FROM schedules
                WHERE {' AND '.join(where)}
                ORDER BY updated_at ASC, created_at ASC
                """,
                tuple(params),
            ).fetchall()
        jobs: list[Any] = []
        for row in rows:
            spec = row["spec_json"] or {}
            if isinstance(spec, str):
                try:
                    spec = json.loads(spec)
                except Exception:
                    spec = {}
            if not isinstance(spec, dict):
                spec = {}
            data = dict(spec)
            data["id"] = str(row["id"])
            data["user_id"] = str(row["user_id"]) if row["user_id"] else data.get("user_id")
            data["session_key"] = str(row["session_key"])
            data["name"] = str(row["name"] or data.get("name") or "")
            data["enabled"] = bool(row["enabled"])
            data["fire_at"] = self._parse_dt(data.get("fire_at"))
            data["created_at"] = self._parse_dt(data.get("created_at") or row["created_at"])
            try:
                jobs.append(ScheduledJob(**data))
            except TypeError:
                allowed = set(ScheduledJob.__dataclass_fields__)
                jobs.append(ScheduledJob(**{key: value for key, value in data.items() if key in allowed}))
        return jobs

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
