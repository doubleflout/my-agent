from __future__ import annotations

import json
import threading
from datetime import timedelta
from typing import Any

import psycopg
from psycopg.rows import dict_row

from core.common.timekit import utcnow


class PostgresProactiveDashboardReader:
    def __init__(self, database_url: str) -> None:
        dsn = database_url.replace("postgresql+psycopg://", "postgresql://", 1)
        self._lock = threading.RLock()
        self._db = psycopg.connect(dsn, row_factory=dict_row)

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def get_overview(self) -> dict[str, Any]:
        counts = {
            "seen_items": self._count("seen_items"),
            "deliveries": self._count("deliveries"),
            "rejection_cooldown": self._count("rejection_cooldown"),
            "semantic_items": self._count("semantic_items"),
            "kv_state": self._count("kv_state"),
            "session_state": self._count("session_state"),
            "context_only_timestamps": self._count("context_only_timestamps"),
            "tick_logs": self._count("tick_log"),
            "tick_steps": self._count("tick_step_log"),
        }
        with self._lock:
            recent_tick = self._db.execute(
                """
                SELECT tick_id, session_key, started_at, finished_at, gate_exit,
                       terminal_action, skip_reason, steps_taken, drift_entered
                FROM tick_log
                ORDER BY started_at DESC
                LIMIT 1
                """
            ).fetchone()
            last_send_at = self._db.execute(
                "SELECT sent_at FROM deliveries ORDER BY sent_at DESC LIMIT 1"
            ).fetchone()
            result_counts_rows = self._db.execute(
                """
                SELECT COALESCE(terminal_action, gate_exit, 'unknown') AS bucket, COUNT(*) AS total
                FROM tick_log
                GROUP BY COALESCE(terminal_action, gate_exit, 'unknown')
                """
            ).fetchall()
            flow_counts_rows = self._db.execute(
                """
                SELECT CASE WHEN drift_entered = TRUE THEN 'drift' ELSE 'proactive' END AS bucket,
                       COUNT(*) AS total
                FROM tick_log
                GROUP BY CASE WHEN drift_entered = TRUE THEN 'drift' ELSE 'proactive' END
                """
            ).fetchall()
        result_counts = {str(row["bucket"]): int(row["total"]) for row in result_counts_rows}
        flow_counts = {str(row["bucket"]): int(row["total"]) for row in flow_counts_rows}
        return {
            "counts": counts,
            "result_counts": result_counts,
            "flow_counts": flow_counts,
            "last_tick_at": recent_tick["started_at"].isoformat() if recent_tick and recent_tick["started_at"] else None,
            "last_send_at": last_send_at["sent_at"].isoformat() if last_send_at and last_send_at["sent_at"] else None,
            "last_skip_reason": (
                recent_tick["skip_reason"]
                if recent_tick is not None and recent_tick["terminal_action"] != "reply"
                else None
            ),
            "recent_tick": self._row_to_tick_log(recent_tick) if recent_tick is not None else None,
        }

    def list_deliveries(self, *, session_key: str = "", sent_from: str = "", sent_to: str = "", page: int = 1, page_size: int = 50) -> tuple[list[dict[str, Any]], int]:
        where, params = self._build_filters(
            ("session_key = %s", session_key),
            ("sent_at >= %s", sent_from),
            ("sent_at <= %s", sent_to),
        )
        return self._list_rows(
            table="deliveries",
            where=where,
            params=params,
            order_by="sent_at DESC, session_key ASC, delivery_key ASC",
            page=page,
            page_size=page_size,
            columns="session_key, delivery_key, sent_at",
        )

    def list_seen_items(self, *, source_key: str = "", page: int = 1, page_size: int = 50) -> tuple[list[dict[str, Any]], int]:
        where, params = self._build_filters(("source_key = %s", source_key))
        return self._list_rows(
            table="seen_items",
            where=where,
            params=params,
            order_by="seen_at DESC, source_key ASC, item_id ASC",
            page=page,
            page_size=page_size,
            columns="source_key, item_id, seen_at",
        )

    def list_rejection_cooldown(self, *, source_key: str = "", page: int = 1, page_size: int = 50) -> tuple[list[dict[str, Any]], int]:
        where, params = self._build_filters(("source_key = %s", source_key))
        return self._list_rows(
            table="rejection_cooldown",
            where=where,
            params=params,
            order_by="rejected_at DESC, source_key ASC, item_id ASC",
            page=page,
            page_size=page_size,
            columns="source_key, item_id, rejected_at",
        )

    def list_semantic_items(self, *, window_hours: int = 168, page: int = 1, page_size: int = 50) -> tuple[list[dict[str, Any]], int]:
        cutoff = utcnow() - timedelta(hours=max(window_hours, 1))
        where, params = self._build_filters(("ts >= %s", cutoff))
        return self._list_rows(
            table="semantic_items",
            where=where,
            params=params,
            order_by="ts DESC, id DESC",
            page=page,
            page_size=page_size,
            columns="id, source_key, item_id, text, ts",
        )

    def list_tick_logs(
        self,
        *,
        session_key: str = "",
        terminal_action: str = "",
        gate_exit: str = "",
        flow: str = "",
        started_from: str = "",
        started_to: str = "",
        page: int = 1,
        page_size: int = 50,
        sort_by: str = "started_at",
        sort_order: str = "desc",
    ) -> tuple[list[dict[str, Any]], int]:
        drift_only: bool | None = None
        if flow == "drift":
            drift_only = True
        elif flow == "proactive":
            drift_only = False
        safe_sort_by = sort_by if sort_by in {
            "session_key", "started_at", "finished_at", "terminal_action",
            "gate_exit", "steps_taken", "alert_count", "content_count",
            "context_count", "drift_entered",
        } else "started_at"
        safe_sort_order = "ASC" if str(sort_order).lower() == "asc" else "DESC"
        where, params = self._build_filters(
            ("session_key = %s", session_key),
            ("terminal_action = %s", terminal_action),
            ("gate_exit = %s", gate_exit),
            ("drift_entered = %s", drift_only),
            ("started_at >= %s", started_from),
            ("started_at <= %s", started_to),
        )
        return self._list_rows(
            table="tick_log",
            where=where,
            params=params,
            order_by=f"{safe_sort_by} {safe_sort_order}, id DESC",
            page=page,
            page_size=page_size,
            columns=(
                "tick_id, session_key, started_at, finished_at, gate_exit, "
                "terminal_action, skip_reason, steps_taken, alert_count, "
                "content_count, context_count, interesting_ids, discarded_ids, "
                "cited_ids, drift_entered, final_message"
            ),
            row_mapper=self._row_to_tick_log,
        )

    def get_tick_log(self, tick_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._db.execute(
                """
                SELECT tick_id, session_key, started_at, finished_at, gate_exit,
                       terminal_action, skip_reason, steps_taken, alert_count,
                       content_count, context_count, interesting_ids, discarded_ids,
                       cited_ids, drift_entered, final_message
                FROM tick_log
                WHERE tick_id = %s
                """,
                (tick_id,),
            ).fetchone()
        return self._row_to_tick_log(row) if row is not None else None

    def list_tick_steps(self, tick_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._db.execute(
                """
                SELECT step_index, phase, tool_name, tool_call_id, tool_args_json,
                       tool_result_text, terminal_action_after, skip_reason_after,
                       interesting_ids_after, discarded_ids_after, cited_ids_after,
                       final_message_after
                FROM tick_step_log
                WHERE tick_id = %s
                ORDER BY step_index ASC, id ASC
                """,
                (tick_id,),
            ).fetchall()
        return [self._row_to_tick_step(row) for row in rows]

    def delete_seen_items(self, *, source_key: str = "", item_ids: list[str] | None = None) -> int:
        return self._delete_rows("seen_items", source_key=source_key, item_ids=item_ids)

    def delete_rejection_cooldown(self, *, source_key: str = "", item_ids: list[str] | None = None) -> int:
        return self._delete_rows("rejection_cooldown", source_key=source_key, item_ids=item_ids)

    def _delete_rows(self, table: str, *, source_key: str = "", item_ids: list[str] | None = None) -> int:
        if not source_key and not item_ids:
            raise ValueError("at least source_key or item_ids is required")
        clauses: list[str] = []
        params: list[Any] = []
        if source_key:
            clauses.append("source_key = %s")
            params.append(source_key)
        if item_ids:
            placeholders = ", ".join("%s" for _ in item_ids)
            clauses.append(f"item_id IN ({placeholders})")
            params.extend(item_ids)
        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            result = self._db.execute(f"DELETE FROM {table}{where_sql}", tuple(params))
            self._db.commit()
        return int(result.rowcount or 0)

    def _list_rows(
        self,
        *,
        table: str,
        where: str,
        params: tuple[Any, ...],
        order_by: str,
        page: int,
        page_size: int,
        columns: str,
        row_mapper=None,
    ) -> tuple[list[dict[str, Any]], int]:
        safe_page = max(1, page)
        safe_size = max(1, min(page_size, 200))
        offset = (safe_page - 1) * safe_size
        with self._lock:
            total_row = self._db.execute(f"SELECT COUNT(*) AS c FROM {table}{where}", params).fetchone()
            rows = self._db.execute(
                f"""
                SELECT {columns}
                FROM {table}{where}
                ORDER BY {order_by}
                LIMIT %s OFFSET %s
                """,
                (*params, safe_size, offset),
            ).fetchall()
        total = int((total_row["c"] if total_row else 0) or 0)
        mapper = row_mapper or self._row_to_dict
        return [mapper(row) for row in rows], total

    def _build_filters(self, *filters: tuple[str, Any]) -> tuple[str, tuple[Any, ...]]:
        clauses: list[str] = []
        params: list[Any] = []
        for clause, value in filters:
            if value is None:
                continue
            if isinstance(value, str) and not value.strip():
                continue
            clauses.append(clause)
            params.append(value)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        return where, tuple(params)

    def _count(self, table: str) -> int:
        with self._lock:
            row = self._db.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
        return int((row["c"] if row else 0) or 0)

    @staticmethod
    def _row_to_dict(row: dict[str, Any]) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for key, value in row.items():
            payload[key] = value.isoformat() if hasattr(value, "isoformat") else value
        return payload

    @staticmethod
    def _decode_json_list(raw: Any) -> list[str]:
        if isinstance(raw, list):
            return [str(item) for item in raw]
        text = str(raw or "").strip()
        if not text:
            return []
        try:
            value = json.loads(text)
        except Exception:
            return []
        return [str(item) for item in value] if isinstance(value, list) else []

    @staticmethod
    def _decode_json_object(raw: Any) -> dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        text = str(raw or "").strip()
        if not text:
            return {}
        try:
            value = json.loads(text)
        except Exception:
            return {}
        return value if isinstance(value, dict) else {}

    def _row_to_tick_log(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = self._row_to_dict(row)
        payload["interesting_ids"] = self._decode_json_list(row.get("interesting_ids"))
        payload["discarded_ids"] = self._decode_json_list(row.get("discarded_ids"))
        payload["cited_ids"] = self._decode_json_list(row.get("cited_ids"))
        payload["drift_entered"] = bool(row.get("drift_entered"))
        return payload

    def _row_to_tick_step(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = self._row_to_dict(row)
        payload["tool_args"] = self._decode_json_object(row.get("tool_args_json"))
        payload.pop("tool_args_json", None)
        payload["interesting_ids_after"] = self._decode_json_list(row.get("interesting_ids_after"))
        payload["discarded_ids_after"] = self._decode_json_list(row.get("discarded_ids_after"))
        payload["cited_ids_after"] = self._decode_json_list(row.get("cited_ids_after"))
        return payload
