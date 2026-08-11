from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from core.common.timekit import parse_iso as _parse_iso, utcnow as _utcnow
from proactive_v2.state import ProactiveStateStore, _dedupe_source_key
from proactive_v2.user_tick import user_id_from_web_session_key

logger = logging.getLogger(__name__)


def _split_session_key(session_key: str) -> tuple[str, str]:
    parts = str(session_key or "").split(":")
    if len(parts) >= 4 and parts[0] == "web" and parts[1] == "proactive":
        return "web_proactive", parts[-1] or session_key
    if len(parts) >= 3 and parts[0] == "web":
        return "web", parts[-1] or session_key
    if ":" not in session_key:
        return "unknown", session_key
    channel, chat_id = session_key.split(":", 1)
    return channel or "unknown", chat_id or session_key


def _web_user_id_from_session_key(session_key: str) -> str | None:
    return user_id_from_web_session_key(session_key)


class PostgresProactiveStateStore(ProactiveStateStore):
    def __init__(self, database_url: str, *, workspace_dir: Path) -> None:
        self.database_url = database_url
        self.workspace_dir = Path(workspace_dir)
        self.workspace_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        dsn = database_url.replace("postgresql+psycopg://", "postgresql://", 1)
        self._db = psycopg.connect(dsn, row_factory=dict_row)
        logger.info(
            "[proactive.state] initialized postgres db=%s seen=%d deliveries=%d semantic=%d reject=%d",
            self.database_url,
            self._count_rows("seen_items"),
            self._count_rows("deliveries"),
            self._count_rows("semantic_items"),
            self._count_rows("rejection_cooldown"),
        )

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def _commit(self) -> None:
        self._db.commit()

    def _existing_web_user_id(self, session_key: str) -> str | None:
        user_id = _web_user_id_from_session_key(session_key)
        if not user_id:
            return None
        row = self._db.execute("SELECT 1 FROM users WHERE id = %s", (user_id,)).fetchone()
        return user_id if row is not None else None

    def _ensure_session_row(self, session_key: str) -> None:
        channel, chat_id = _split_session_key(session_key)
        now = _utcnow()
        user_id = self._existing_web_user_id(session_key)
        self._db.execute(
            """
            INSERT INTO sessions(
                key, user_id, channel, chat_id, created_at, updated_at,
                last_consolidated, metadata, next_seq
            )
            VALUES (%s, %s, %s, %s, %s, %s, 0, %s, 0)
            ON CONFLICT(key) DO UPDATE SET
                user_id = COALESCE(sessions.user_id, excluded.user_id),
                channel = excluded.channel,
                chat_id = excluded.chat_id
            """,
            (session_key, user_id, channel, chat_id, now, now, Jsonb({})),
        )

    def record_tick_log_start(
        self,
        *,
        tick_id: str,
        session_key: str,
        started_at: str,
        gate_exit: str | None = None,
    ) -> None:
        with self._lock:
            self._ensure_session_row(session_key)
            user_id = self._existing_web_user_id(session_key)
            self._db.execute(
                """
                INSERT INTO tick_log(tick_id, session_key, user_id, started_at, gate_exit)
                VALUES(%s, %s, %s, %s, %s)
                ON CONFLICT(tick_id) DO UPDATE SET
                    session_key = excluded.session_key,
                    user_id = COALESCE(tick_log.user_id, excluded.user_id),
                    started_at = excluded.started_at,
                    gate_exit = excluded.gate_exit
                """,
                (tick_id, session_key, user_id, started_at, gate_exit),
            )
            self._commit()

    def record_tick_log_finish(
        self,
        *,
        tick_id: str,
        session_key: str,
        started_at: str,
        finished_at: str,
        gate_exit: str | None,
        terminal_action: str | None,
        skip_reason: str,
        steps_taken: int,
        alert_count: int,
        content_count: int,
        context_count: int,
        interesting_ids: list[str],
        discarded_ids: list[str],
        cited_ids: list[str],
        drift_entered: bool,
        final_message: str,
    ) -> None:
        with self._lock:
            self._ensure_session_row(session_key)
            user_id = self._existing_web_user_id(session_key)
            self._db.execute(
                """
                INSERT INTO tick_log(
                    tick_id, session_key, user_id, started_at, finished_at, gate_exit,
                    terminal_action, skip_reason, steps_taken, alert_count,
                    content_count, context_count, interesting_ids, discarded_ids,
                    cited_ids, drift_entered, final_message
                ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(tick_id) DO UPDATE SET
                    session_key = excluded.session_key,
                    user_id = COALESCE(tick_log.user_id, excluded.user_id),
                    started_at = excluded.started_at,
                    finished_at = excluded.finished_at,
                    gate_exit = excluded.gate_exit,
                    terminal_action = excluded.terminal_action,
                    skip_reason = excluded.skip_reason,
                    steps_taken = excluded.steps_taken,
                    alert_count = excluded.alert_count,
                    content_count = excluded.content_count,
                    context_count = excluded.context_count,
                    interesting_ids = excluded.interesting_ids,
                    discarded_ids = excluded.discarded_ids,
                    cited_ids = excluded.cited_ids,
                    drift_entered = excluded.drift_entered,
                    final_message = excluded.final_message
                """,
                (
                    tick_id,
                    session_key,
                    user_id,
                    started_at,
                    finished_at,
                    gate_exit,
                    terminal_action,
                    skip_reason,
                    steps_taken,
                    alert_count,
                    content_count,
                    context_count,
                    Jsonb(interesting_ids),
                    Jsonb(discarded_ids),
                    Jsonb(cited_ids),
                    bool(drift_entered),
                    final_message,
                ),
            )
            self._commit()

    def record_tick_step_log(
        self,
        *,
        tick_id: str,
        step_index: int,
        phase: str,
        tool_name: str,
        tool_call_id: str,
        tool_args: dict[str, Any],
        tool_result_text: str,
        terminal_action_after: str | None,
        skip_reason_after: str,
        interesting_ids_after: list[str],
        discarded_ids_after: list[str],
        cited_ids_after: list[str],
        final_message_after: str,
    ) -> None:
        with self._lock:
            self._db.execute(
                """
                INSERT INTO tick_step_log(
                    tick_id, step_index, phase, tool_name, tool_call_id,
                    tool_args_json, tool_result_text, terminal_action_after,
                    skip_reason_after, interesting_ids_after, discarded_ids_after,
                    cited_ids_after, final_message_after
                ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(tick_id, step_index, tool_call_id) DO UPDATE SET
                    phase = excluded.phase,
                    tool_name = excluded.tool_name,
                    tool_args_json = excluded.tool_args_json,
                    tool_result_text = excluded.tool_result_text,
                    terminal_action_after = excluded.terminal_action_after,
                    skip_reason_after = excluded.skip_reason_after,
                    interesting_ids_after = excluded.interesting_ids_after,
                    discarded_ids_after = excluded.discarded_ids_after,
                    cited_ids_after = excluded.cited_ids_after,
                    final_message_after = excluded.final_message_after
                """,
                (
                    tick_id,
                    step_index,
                    phase,
                    tool_name,
                    tool_call_id,
                    Jsonb(tool_args),
                    tool_result_text,
                    terminal_action_after,
                    skip_reason_after,
                    Jsonb(interesting_ids_after),
                    Jsonb(discarded_ids_after),
                    Jsonb(cited_ids_after),
                    final_message_after,
                ),
            )
            self._commit()

    def is_item_seen(self, source_key: str, item_id: str, ttl_hours: int, now: datetime | None = None) -> bool:
        now = now or _utcnow()
        dedupe_key = _dedupe_source_key(source_key)
        cutoff = now - timedelta(hours=max(ttl_hours, 1))
        with self._lock:
            row = self._db.execute(
                "SELECT seen_at FROM seen_items WHERE source_key = %s AND item_id = %s",
                (dedupe_key, item_id),
            ).fetchone()
        if row is None:
            return False
        ts = row["seen_at"]
        if ts is None or ts < cutoff:
            return False
        return True

    def mark_items_seen(self, entries: list[tuple[str, str]], now: datetime | None = None) -> None:
        if not entries:
            return
        now = now or _utcnow()
        params = [(_dedupe_source_key(source_key), item_id, now) for source_key, item_id in entries]
        with self._lock:
            self._db.executemany(
                """
                INSERT INTO seen_items(source_key, item_id, seen_at)
                VALUES(%s, %s, %s)
                ON CONFLICT(source_key, item_id) DO UPDATE SET seen_at = excluded.seen_at
                """,
                params,
            )
            self._commit()

    def is_delivery_duplicate(
        self,
        session_key: str,
        delivery_key: str,
        window_hours: int,
        now: datetime | None = None,
    ) -> bool:
        now = now or _utcnow()
        cutoff = now - timedelta(hours=max(window_hours, 1))
        with self._lock:
            row = self._db.execute(
                "SELECT sent_at FROM deliveries WHERE session_key = %s AND delivery_key = %s",
                (session_key, delivery_key),
            ).fetchone()
        if row is None:
            return False
        ts = row["sent_at"]
        return ts is not None and ts >= cutoff

    def mark_delivery(self, session_key: str, delivery_key: str, now: datetime | None = None) -> None:
        now = now or _utcnow()
        with self._lock:
            self._ensure_session_row(session_key)
            self._db.execute(
                """
                INSERT INTO deliveries(session_key, delivery_key, sent_at)
                VALUES(%s, %s, %s)
                ON CONFLICT(session_key, delivery_key) DO UPDATE SET sent_at = excluded.sent_at
                """,
                (session_key, delivery_key, now),
            )
            self._commit()

    def count_deliveries_in_window(self, session_key: str, window_hours: int, now: datetime | None = None) -> int:
        now = now or _utcnow()
        cutoff = now - timedelta(hours=window_hours)
        with self._lock:
            row = self._db.execute(
                "SELECT COUNT(*) AS c FROM deliveries WHERE session_key = %s AND sent_at >= %s",
                (session_key, cutoff),
            ).fetchone()
        return int((row["c"] if row else 0) or 0)

    def get_semantic_items(
        self,
        window_hours: int,
        max_candidates: int,
        now: datetime | None = None,
    ) -> list[dict[str, str]]:
        now = now or _utcnow()
        cutoff = now - timedelta(hours=window_hours)
        with self._lock:
            rows = self._db.execute(
                """
                SELECT source_key, item_id, text, ts
                FROM semantic_items
                WHERE ts >= %s
                ORDER BY ts DESC
                LIMIT %s
                """,
                (cutoff, max(max_candidates, 1)),
            ).fetchall()
        return [
            {
                "source_key": str(row["source_key"]),
                "item_id": str(row["item_id"]),
                "text": str(row["text"]),
                "ts": row["ts"].isoformat() if row["ts"] else "",
            }
            for row in rows
            if str(row["text"]).strip()
        ]

    def mark_semantic_items(self, entries: list[dict[str, str]], now: datetime | None = None) -> None:
        if not entries:
            return
        now = now or _utcnow()
        params: list[tuple[str, str, str, datetime]] = []
        for entry in entries:
            text = str(entry.get("text", "")).strip()
            if not text:
                continue
            params.append((str(entry.get("source_key", "")), str(entry.get("item_id", "")), text, now))
        if not params:
            return
        with self._lock:
            self._db.executemany(
                "INSERT INTO semantic_items(source_key, item_id, text, ts) VALUES(%s, %s, %s, %s)",
                params,
            )
            self._commit()

    def is_rejection_cooled(
        self,
        source_key: str,
        item_id: str,
        ttl_hours: int,
        now: datetime | None = None,
    ) -> bool:
        if ttl_hours <= 0:
            return False
        now = now or _utcnow()
        dedupe_key = _dedupe_source_key(source_key)
        cutoff = now - timedelta(hours=ttl_hours)
        with self._lock:
            row = self._db.execute(
                "SELECT rejected_at FROM rejection_cooldown WHERE source_key = %s AND item_id = %s",
                (dedupe_key, item_id),
            ).fetchone()
        if row is None:
            return False
        ts = row["rejected_at"]
        return ts is not None and ts >= cutoff

    def mark_rejection_cooldown(
        self,
        entries: list[tuple[str, str]],
        hours: int,
        now: datetime | None = None,
    ) -> None:
        if hours <= 0 or not entries:
            return
        now = now or _utcnow()
        params = [(_dedupe_source_key(source_key), item_id, now) for source_key, item_id in entries]
        with self._lock:
            self._db.executemany(
                """
                INSERT INTO rejection_cooldown(source_key, item_id, rejected_at)
                VALUES(%s, %s, %s)
                ON CONFLICT(source_key, item_id) DO UPDATE SET rejected_at = excluded.rejected_at
                """,
                params,
            )
            self._commit()

    def cleanup(
        self,
        seen_ttl_hours: int,
        delivery_ttl_hours: int,
        semantic_ttl_hours: int,
        rejection_cooldown_ttl_hours: int = 0,
    ) -> None:
        now = _utcnow()
        seen_cutoff = now - timedelta(hours=max(seen_ttl_hours, 1))
        delivery_cutoff = now - timedelta(hours=max(delivery_ttl_hours, 1))
        semantic_cutoff = now - timedelta(hours=max(semantic_ttl_hours, 1))
        context_only_cutoff = now - timedelta(hours=24)
        with self._lock:
            self._db.execute("DELETE FROM seen_items WHERE seen_at < %s", (seen_cutoff,))
            self._db.execute("DELETE FROM deliveries WHERE sent_at < %s", (delivery_cutoff,))
            self._db.execute("DELETE FROM semantic_items WHERE ts < %s OR BTRIM(text) = ''", (semantic_cutoff,))
            if rejection_cooldown_ttl_hours > 0:
                cooldown_cutoff = now - timedelta(hours=rejection_cooldown_ttl_hours)
                self._db.execute(
                    "DELETE FROM rejection_cooldown WHERE rejected_at < %s",
                    (cooldown_cutoff,),
                )
            self._db.execute("DELETE FROM context_only_timestamps WHERE ts < %s", (context_only_cutoff,))
            self._commit()

    def get_bg_context_last_main_at(self) -> datetime | None:
        return self._get_kv_datetime("bg_context_last_main_at")

    def mark_bg_context_main_send(self, now: datetime | None = None) -> None:
        now = now or _utcnow()
        self._set_kv("bg_context_last_main_at", now.isoformat())

    def get_last_drift_at(self, session_key: str) -> datetime | None:
        return self._get_session_datetime(session_key, "drift_last_at")

    def mark_drift_run(self, session_key: str, now: datetime | None = None) -> None:
        now = now or _utcnow()
        self._set_session_state(session_key, "drift_last_at", now.isoformat())

    def get_last_context_only_at(self, session_key: str) -> datetime | None:
        return self._get_session_datetime(session_key, "context_only_last_at")

    def mark_context_only_send(self, session_key: str, now: datetime | None = None) -> None:
        now = now or _utcnow()
        ts = now.isoformat()
        with self._lock:
            self._ensure_session_row(session_key)
            self._db.execute(
                """
                INSERT INTO session_state(session_key, key, value)
                VALUES(%s, %s, %s)
                ON CONFLICT(session_key, key) DO UPDATE SET value = excluded.value
                """,
                (session_key, "context_only_last_at", ts),
            )
            self._db.execute(
                """
                INSERT INTO context_only_timestamps(session_key, ts)
                VALUES(%s, %s)
                ON CONFLICT(session_key, ts) DO NOTHING
                """,
                (session_key, now),
            )
            self._commit()

    def count_context_only_in_window(self, session_key: str, window_hours: int, now: datetime | None = None) -> int:
        now = now or _utcnow()
        cutoff = now - timedelta(hours=window_hours)
        with self._lock:
            row = self._db.execute(
                """
                SELECT COUNT(*) AS c
                FROM context_only_timestamps
                WHERE session_key = %s AND ts >= %s
                """,
                (session_key, cutoff),
            ).fetchone()
        return int((row["c"] if row else 0) or 0)

    def _get_kv_datetime(self, key: str) -> datetime | None:
        with self._lock:
            row = self._db.execute("SELECT value FROM kv_state WHERE key = %s", (key,)).fetchone()
        return _parse_iso(str(row["value"])) if row is not None else None

    def _set_kv(self, key: str, value: str) -> None:
        with self._lock:
            self._db.execute(
                """
                INSERT INTO kv_state(key, value)
                VALUES(%s, %s)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )
            self._commit()

    def _get_session_datetime(self, session_key: str, key: str) -> datetime | None:
        with self._lock:
            row = self._db.execute(
                "SELECT value FROM session_state WHERE session_key = %s AND key = %s",
                (session_key, key),
            ).fetchone()
        return _parse_iso(str(row["value"])) if row is not None else None

    def _set_session_state(self, session_key: str, key: str, value: str) -> None:
        with self._lock:
            self._ensure_session_row(session_key)
            self._db.execute(
                """
                INSERT INTO session_state(session_key, key, value)
                VALUES(%s, %s, %s)
                ON CONFLICT(session_key, key) DO UPDATE SET value = excluded.value
                """,
                (session_key, key, value),
            )
            self._commit()

    def _count_rows(self, table: str) -> int:
        with self._lock:
            row = self._db.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
        return int((row["c"] if row else 0) or 0)
