from __future__ import annotations

import json
import threading
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


def _now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        if not value:
            return default
        try:
            return json.loads(value)
        except Exception:
            return default
    return value


def _jsonb(value: Any) -> Jsonb:
    return Jsonb(value if value is not None else {})


def _split_session_key(session_key: str) -> tuple[str, str]:
    if ":" not in session_key:
        return "unknown", session_key
    channel, chat_id = session_key.split(":", 1)
    return channel or "unknown", chat_id or session_key


def _web_user_id_from_session_key(session_key: str) -> str | None:
    parts = session_key.split(":", 2)
    if len(parts) == 3 and parts[0] == "web" and parts[1]:
        return parts[1]
    return None


class PostgresSessionStore:
    """PostgreSQL implementation compatible with session.store.SessionStore.

    The public method names intentionally match the SQLite store so AgentLoop,
    SessionManager, tools, and dashboard code can switch storage without knowing
    which database backs sessions/messages.
    """

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        dsn = database_url.replace("postgresql+psycopg://", "postgresql://", 1)
        self._lock = threading.RLock()
        self._conn = psycopg.connect(dsn, row_factory=dict_row)

    @property
    def db_path(self) -> str:
        return self.database_url

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _commit(self) -> None:
        self._conn.commit()

    def _rollback(self) -> None:
        self._conn.rollback()

    def _session_user_id(self, session_key: str) -> str | None:
        row = self._conn.execute(
            "SELECT user_id FROM sessions WHERE key = %s",
            (session_key,),
        ).fetchone()
        if row and row.get("user_id"):
            return str(row["user_id"])
        return self._existing_web_user_id(session_key)

    def _existing_web_user_id(self, session_key: str) -> str | None:
        user_id = _web_user_id_from_session_key(session_key)
        if not user_id:
            return None
        row = self._conn.execute("SELECT 1 FROM users WHERE id = %s", (user_id,)).fetchone()
        return user_id if row is not None else None

    def _ensure_session_row(self, session_key: str) -> None:
        channel, chat_id = _split_session_key(session_key)
        now = _now_iso()
        user_id = self._existing_web_user_id(session_key)
        self._conn.execute(
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

    def session_exists(self, key: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM sessions WHERE key = %s",
                (key,),
            ).fetchone()
        return row is not None

    def upsert_session(
        self,
        key: str,
        *,
        created_at: str,
        updated_at: str,
        last_consolidated: int,
        metadata: dict[str, Any],
    ) -> None:
        channel, chat_id = _split_session_key(key)
        user_id = self._existing_web_user_id(key)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO sessions(
                    key, user_id, channel, chat_id, created_at, updated_at,
                    last_consolidated, metadata
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(key) DO UPDATE SET
                    user_id = COALESCE(sessions.user_id, excluded.user_id),
                    channel = excluded.channel,
                    chat_id = excluded.chat_id,
                    updated_at = excluded.updated_at,
                    last_consolidated = excluded.last_consolidated,
                    metadata = excluded.metadata
                """,
                (
                    key,
                    user_id,
                    channel,
                    chat_id,
                    created_at,
                    updated_at,
                    int(last_consolidated),
                    _jsonb(metadata or {}),
                ),
            )
            self._commit()

    def update_last_consolidated(self, key: str, last_consolidated: int) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE sessions
                SET last_consolidated = %s, updated_at = %s
                WHERE key = %s
                """,
                (int(last_consolidated), _now_iso(), key),
            )
            self._commit()

    def get_session_meta(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT key, created_at, updated_at, last_consolidated, metadata,
                       last_user_at, last_proactive_at, next_seq
                FROM sessions
                WHERE key = %s
                """,
                (key,),
            ).fetchone()
        if row is None:
            return None
        return {
            "key": str(row["key"]),
            "created_at": row["created_at"].isoformat() if row["created_at"] else "",
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else "",
            "last_consolidated": int(row["last_consolidated"] or 0),
            "metadata": _json_value(row["metadata"], {}),
            "last_user_at": row["last_user_at"].isoformat() if row["last_user_at"] else None,
            "last_proactive_at": row["last_proactive_at"].isoformat() if row["last_proactive_at"] else None,
            "next_seq": int(row["next_seq"] or 0),
        }

    def list_sessions(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT key, created_at, updated_at, last_user_at, last_proactive_at
                FROM sessions
                ORDER BY updated_at DESC
                """
            ).fetchall()
        return [
            {
                "key": str(row["key"]),
                "created_at": row["created_at"].isoformat() if row["created_at"] else "",
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else "",
                "last_user_at": row["last_user_at"].isoformat() if row["last_user_at"] else None,
                "last_proactive_at": row["last_proactive_at"].isoformat() if row["last_proactive_at"] else None,
            }
            for row in rows
        ]

    def list_sessions_for_dashboard(
        self,
        *,
        q: str = "",
        channel: str = "",
        updated_from: str = "",
        updated_to: str = "",
        has_proactive: bool | None = None,
        page: int = 1,
        page_size: int = 50,
        sort_by: str = "updated_at",
        sort_order: str = "desc",
    ) -> tuple[list[dict[str, Any]], int]:
        safe_page = max(1, int(page))
        safe_page_size = max(1, min(int(page_size), 200))
        offset = (safe_page - 1) * safe_page_size
        safe_sort_by = sort_by if sort_by in {
            "updated_at",
            "created_at",
            "last_user_at",
            "last_proactive_at",
        } else "updated_at"
        safe_sort_order = "ASC" if str(sort_order).lower() == "asc" else "DESC"

        params: list[Any] = []
        where_parts: list[str] = []
        query = (q or "").strip()
        if query:
            where_parts.append("(s.key ILIKE %s OR s.metadata::text ILIKE %s)")
            like = f"%{query}%"
            params.extend([like, like])
        if channel:
            where_parts.append("s.key LIKE %s")
            params.append(f"{channel}:%")
        if updated_from:
            where_parts.append("s.updated_at >= %s")
            params.append(updated_from)
        if updated_to:
            where_parts.append("s.updated_at <= %s")
            params.append(updated_to)
        if has_proactive is True:
            where_parts.append("s.last_proactive_at IS NOT NULL")
        if has_proactive is False:
            where_parts.append("s.last_proactive_at IS NULL")

        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        with self._lock:
            count_row = self._conn.execute(
                f"SELECT COUNT(1) AS c FROM sessions s {where_sql}",
                tuple(params),
            ).fetchone()
            rows = self._conn.execute(
                f"""
                SELECT
                    s.key,
                    s.created_at,
                    s.updated_at,
                    s.last_consolidated,
                    s.metadata,
                    s.last_user_at,
                    s.last_proactive_at,
                    COALESCE(msg.message_count, 0) AS message_count
                FROM sessions s
                LEFT JOIN (
                    SELECT session_key, COUNT(1) AS message_count
                    FROM messages
                    GROUP BY session_key
                ) msg ON msg.session_key = s.key
                {where_sql}
                ORDER BY s.{safe_sort_by} {safe_sort_order}, s.key ASC
                LIMIT %s OFFSET %s
                """,
                tuple([*params, safe_page_size, offset]),
            ).fetchall()
        total = int((count_row["c"] if count_row else 0) or 0)
        return [
            {
                "key": str(row["key"]),
                "created_at": row["created_at"].isoformat() if row["created_at"] else "",
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else "",
                "last_consolidated": int(row["last_consolidated"] or 0),
                "metadata": _json_value(row["metadata"], {}),
                "last_user_at": row["last_user_at"].isoformat() if row["last_user_at"] else None,
                "last_proactive_at": row["last_proactive_at"].isoformat() if row["last_proactive_at"] else None,
                "message_count": int(row["message_count"] or 0),
            }
            for row in rows
        ], total

    def create_session(
        self,
        *,
        key: str,
        metadata: dict[str, Any] | None = None,
        last_consolidated: int = 0,
        last_user_at: str | None = None,
        last_proactive_at: str | None = None,
    ) -> dict[str, Any]:
        now = _now_iso()
        channel, chat_id = _split_session_key(key)
        user_id = self._existing_web_user_id(key)
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO sessions(
                    key, user_id, channel, chat_id, created_at, updated_at,
                    last_consolidated, metadata, last_user_at, last_proactive_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    key,
                    user_id,
                    channel,
                    chat_id,
                    now,
                    now,
                    int(last_consolidated),
                    _jsonb(metadata or {}),
                    last_user_at,
                    last_proactive_at,
                ),
            )
            self._commit()
        meta = self.get_session_meta(key)
        if meta is None:
            raise ValueError(f"session create failed: {key}")
        return meta

    def update_session(
        self,
        key: str,
        *,
        metadata: dict[str, Any] | None = None,
        last_consolidated: int | None = None,
        last_user_at: str | None = None,
        last_proactive_at: str | None = None,
    ) -> dict[str, Any] | None:
        set_parts = ["updated_at = %s"]
        params: list[Any] = [_now_iso()]
        if metadata is not None:
            set_parts.append("metadata = %s")
            params.append(_jsonb(metadata))
        if last_consolidated is not None:
            set_parts.append("last_consolidated = %s")
            params.append(int(last_consolidated))
        if last_user_at is not None:
            set_parts.append("last_user_at = %s")
            params.append(last_user_at)
        if last_proactive_at is not None:
            set_parts.append("last_proactive_at = %s")
            params.append(last_proactive_at)
        params.append(key)
        with self._lock:
            cur = self._conn.execute(
                f"UPDATE sessions SET {', '.join(set_parts)} WHERE key = %s",
                tuple(params),
            )
            self._commit()
        if cur.rowcount <= 0:
            return None
        return self.get_session_meta(key)

    def delete_session(self, key: str, *, cascade: bool = False) -> bool:
        with self._lock:
            if not cascade:
                row = self._conn.execute(
                    "SELECT COUNT(1) AS c FROM messages WHERE session_key = %s",
                    (key,),
                ).fetchone()
                if int((row["c"] if row else 0) or 0) > 0:
                    raise ValueError("session still has messages; use cascade")
            else:
                self._conn.execute("DELETE FROM messages WHERE session_key = %s", (key,))
            cur = self._conn.execute("DELETE FROM sessions WHERE key = %s", (key,))
            self._commit()
        return cur.rowcount > 0

    def delete_sessions_batch(self, keys: list[str], *, cascade: bool = False) -> int:
        clean_keys = [str(key).strip() for key in keys if str(key).strip()]
        if not clean_keys:
            return 0
        with self._lock:
            if not cascade:
                row = self._conn.execute(
                    "SELECT COUNT(1) AS c FROM messages WHERE session_key = ANY(%s)",
                    (clean_keys,),
                ).fetchone()
                if int((row["c"] if row else 0) or 0) > 0:
                    raise ValueError("selected sessions still have messages; use cascade")
            else:
                self._conn.execute("DELETE FROM messages WHERE session_key = ANY(%s)", (clean_keys,))
            cur = self._conn.execute("DELETE FROM sessions WHERE key = ANY(%s)", (clean_keys,))
            self._commit()
        return int(cur.rowcount or 0)

    def update_presence(
        self,
        key: str,
        *,
        last_user_at: str | None = None,
        last_proactive_at: str | None = None,
    ) -> None:
        channel, chat_id = _split_session_key(key)
        user_id = self._existing_web_user_id(key)
        now = _now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO sessions(
                    key, user_id, channel, chat_id, created_at, updated_at,
                    last_consolidated, metadata, last_user_at, last_proactive_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, 0, %s, %s, %s)
                ON CONFLICT(key) DO UPDATE SET
                    user_id = COALESCE(sessions.user_id, excluded.user_id),
                    updated_at = excluded.updated_at,
                    last_user_at = COALESCE(excluded.last_user_at, sessions.last_user_at),
                    last_proactive_at = COALESCE(excluded.last_proactive_at, sessions.last_proactive_at)
                """,
                (key, user_id, channel, chat_id, now, now, Jsonb({}), last_user_at, last_proactive_at),
            )
            self._commit()

    def get_presence(self, key: str) -> dict[str, str | None] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT last_user_at, last_proactive_at FROM sessions WHERE key = %s",
                (key,),
            ).fetchone()
        if row is None:
            return None
        return {
            "last_user_at": row["last_user_at"].isoformat() if row["last_user_at"] else None,
            "last_proactive_at": row["last_proactive_at"].isoformat() if row["last_proactive_at"] else None,
        }

    def list_presence(self) -> dict[str, dict[str, str | None]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT key, last_user_at, last_proactive_at
                FROM sessions
                WHERE last_user_at IS NOT NULL OR last_proactive_at IS NOT NULL
                """
            ).fetchall()
        return {
            str(row["key"]): {
                "last_user_at": row["last_user_at"].isoformat() if row["last_user_at"] else None,
                "last_proactive_at": row["last_proactive_at"].isoformat() if row["last_proactive_at"] else None,
            }
            for row in rows
        }

    def most_recent_user_at(self) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(last_user_at) AS last_user_at FROM sessions WHERE last_user_at IS NOT NULL"
            ).fetchone()
        return row["last_user_at"].isoformat() if row and row["last_user_at"] else None

    def get_channel_metadata(self, channel: str) -> list[dict[str, Any]]:
        like_key = f"{channel}:%"
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, metadata FROM sessions WHERE key LIKE %s",
                (like_key,),
            ).fetchall()
        return [
            {
                "key": str(row["key"]),
                "chat_id": str(row["key"]).split(":", 1)[-1] if ":" in str(row["key"]) else str(row["key"]),
                "metadata": _json_value(row["metadata"], {}),
            }
            for row in rows
        ]

    def count_messages(self, session_key: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(1) AS c FROM messages WHERE session_key = %s",
                (session_key,),
            ).fetchone()
        return int((row["c"] if row else 0) or 0)

    def next_seq(self, session_key: str) -> int:
        with self._lock:
            self._ensure_session_row(session_key)
            row = self._conn.execute(
                """
                SELECT GREATEST(
                    COALESCE((SELECT next_seq FROM sessions WHERE key = %s), 0),
                    COALESCE((SELECT MAX(seq) + 1 FROM messages WHERE session_key = %s), 0)
                ) AS next_seq
                """,
                (session_key, session_key),
            ).fetchone()
            self._commit()
        return int((row["next_seq"] if row else 0) or 0)

    def insert_message(
        self,
        session_key: str,
        *,
        role: str,
        content: str,
        ts: str,
        seq: int,
        tool_chain: Any | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        extra_payload = extra or {}
        with self._lock:
            try:
                self._ensure_session_row(session_key)
                locked = self._conn.execute(
                    "SELECT next_seq FROM sessions WHERE key = %s FOR UPDATE",
                    (session_key,),
                ).fetchone()
                current_next = int((locked["next_seq"] if locked else 0) or 0)
                max_row = self._conn.execute(
                    "SELECT COALESCE(MAX(seq) + 1, 0) AS next_seq FROM messages WHERE session_key = %s",
                    (session_key,),
                ).fetchone()
                actual_seq = max(int(seq), current_next, int((max_row["next_seq"] if max_row else 0) or 0))
                message_id = f"{session_key}:{actual_seq}"
                user_id = self._session_user_id(session_key)
                self._conn.execute(
                    """
                    INSERT INTO messages(id, session_key, user_id, seq, role, content, tool_chain, extra, ts)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(id) DO NOTHING
                    """,
                    (
                        message_id,
                        session_key,
                        user_id,
                        actual_seq,
                        role,
                        content,
                        Jsonb(tool_chain) if tool_chain is not None else None,
                        Jsonb(extra_payload),
                        ts,
                    ),
                )
                self._conn.execute(
                    """
                    UPDATE sessions
                    SET next_seq = GREATEST(next_seq, %s), updated_at = %s
                    WHERE key = %s
                    """,
                    (actual_seq + 1, _now_iso(), session_key),
                )
                self._commit()
            except Exception:
                self._rollback()
                raise
        row: dict[str, Any] = {
            "id": message_id,
            "session_key": session_key,
            "seq": actual_seq,
            "role": role,
            "content": content,
            "timestamp": ts,
        }
        if tool_chain is not None:
            row["tool_chain"] = tool_chain
        if extra:
            row.update(extra)
        return row

    def fetch_session_messages(self, session_key: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, session_key, seq, role, content, tool_chain, extra, ts
                FROM messages
                WHERE session_key = %s
                ORDER BY seq ASC
                """,
                (session_key,),
            ).fetchall()
        return [self._row_to_message(row) for row in rows]

    def list_messages_for_dashboard(
        self,
        *,
        session_key: str | None = None,
        q: str = "",
        role: str = "",
        page: int = 1,
        page_size: int = 25,
        sort_by: str = "ts",
        sort_order: str = "desc",
    ) -> tuple[list[dict[str, Any]], int]:
        safe_page = max(1, int(page))
        safe_page_size = max(1, min(int(page_size), 200))
        offset = (safe_page - 1) * safe_page_size
        safe_sort = "ASC" if str(sort_order).lower() == "asc" else "DESC"
        safe_sort_by = sort_by if sort_by in {"ts", "seq", "role", "session_key"} else "ts"
        params: list[Any] = []
        where_parts: list[str] = []
        if session_key:
            where_parts.append("session_key = %s")
            params.append(session_key)
        term = (q or "").strip()
        if term:
            where_parts.append("content ILIKE %s")
            params.append(f"%{term}%")
        if role:
            where_parts.append("role = %s")
            params.append(role)
        where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
        with self._lock:
            count_row = self._conn.execute(
                f"SELECT COUNT(1) AS c FROM messages {where_sql}",
                tuple(params),
            ).fetchone()
            rows = self._conn.execute(
                f"""
                SELECT id, session_key, seq, role, content, tool_chain, extra, ts
                FROM messages
                {where_sql}
                ORDER BY {safe_sort_by} {safe_sort}, seq {safe_sort}, id ASC
                LIMIT %s OFFSET %s
                """,
                tuple([*params, safe_page_size, offset]),
            ).fetchall()
        total = int((count_row["c"] if count_row else 0) or 0)
        return [self._row_to_message(row) for row in rows], total

    def get_message(self, message_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT id, session_key, seq, role, content, tool_chain, extra, ts
                FROM messages
                WHERE id = %s
                """,
                (message_id,),
            ).fetchone()
        return self._row_to_message(row) if row else None

    def update_message(
        self,
        message_id: str,
        *,
        role: str | None = None,
        content: str | None = None,
        tool_chain: Any | None = None,
        extra: dict[str, Any] | None = None,
        ts: str | None = None,
    ) -> dict[str, Any] | None:
        set_parts: list[str] = []
        params: list[Any] = []
        if role is not None:
            set_parts.append("role = %s")
            params.append(role)
        if content is not None:
            set_parts.append("content = %s")
            params.append(content)
        if tool_chain is not None:
            set_parts.append("tool_chain = %s")
            params.append(Jsonb(tool_chain))
        if extra is not None:
            set_parts.append("extra = %s")
            params.append(Jsonb(extra))
        if ts is not None:
            set_parts.append("ts = %s")
            params.append(ts)
        if not set_parts:
            return self.get_message(message_id)
        with self._lock:
            row = self._conn.execute(
                "SELECT session_key FROM messages WHERE id = %s",
                (message_id,),
            ).fetchone()
            if row is None:
                return None
            session_key = str(row["session_key"])
            params.append(message_id)
            cur = self._conn.execute(
                f"UPDATE messages SET {', '.join(set_parts)} WHERE id = %s",
                tuple(params),
            )
            self._conn.execute(
                "UPDATE sessions SET updated_at = %s WHERE key = %s",
                (_now_iso(), session_key),
            )
            self._commit()
        if cur.rowcount <= 0:
            return None
        return self.get_message(message_id)

    def delete_message(self, message_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT session_key FROM messages WHERE id = %s",
                (message_id,),
            ).fetchone()
            if row is None:
                return False
            session_key = str(row["session_key"])
            cur = self._conn.execute("DELETE FROM messages WHERE id = %s", (message_id,))
            self._conn.execute(
                "UPDATE sessions SET updated_at = %s WHERE key = %s",
                (_now_iso(), session_key),
            )
            self._commit()
        return cur.rowcount > 0

    def delete_messages_batch(self, ids: list[str]) -> int:
        clean_ids = [str(message_id).strip() for message_id in ids if str(message_id).strip()]
        if not clean_ids:
            return 0
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT session_key FROM messages WHERE id = ANY(%s)",
                (clean_ids,),
            ).fetchall()
            cur = self._conn.execute("DELETE FROM messages WHERE id = ANY(%s)", (clean_ids,))
            now = _now_iso()
            for row in rows:
                self._conn.execute(
                    "UPDATE sessions SET updated_at = %s WHERE key = %s",
                    (now, str(row["session_key"])),
                )
            self._commit()
        return int(cur.rowcount or 0)

    def delete_session_messages_and_update_cursor(
        self,
        session_key: str,
        *,
        ids: list[str],
        last_consolidated: int,
    ) -> int:
        clean_ids = [str(message_id).strip() for message_id in ids if str(message_id).strip()]
        if not clean_ids:
            return 0
        with self._lock:
            try:
                seq_rows = self._conn.execute(
                    """
                    SELECT seq
                    FROM messages
                    WHERE session_key = %s AND id = ANY(%s)
                    """,
                    (session_key, clean_ids),
                ).fetchall()
                next_seq = max([int(row["seq"]) for row in seq_rows], default=-1) + 1
                cur = self._conn.execute(
                    """
                    DELETE FROM messages
                    WHERE session_key = %s AND id = ANY(%s)
                    """,
                    (session_key, clean_ids),
                )
                self._conn.execute(
                    """
                    UPDATE sessions
                    SET last_consolidated = %s,
                        updated_at = %s,
                        next_seq = GREATEST(next_seq, %s)
                    WHERE key = %s
                    """,
                    (int(last_consolidated), _now_iso(), next_seq, session_key),
                )
                self._commit()
            except Exception:
                self._rollback()
                raise
        return int(cur.rowcount or 0)

    def fetch_by_ids(self, ids: list[str]) -> list[dict[str, Any]]:
        if not ids:
            return []
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, session_key, seq, role, content, tool_chain, extra, ts
                FROM messages
                WHERE id = ANY(%s)
                """,
                (ids,),
            ).fetchall()
        by_id = {str(row["id"]): self._row_to_message(row) for row in rows}
        return [by_id[msg_id] for msg_id in ids if msg_id in by_id]

    def fetch_by_ids_with_context(self, ids: list[str], context: int) -> list[dict[str, Any]]:
        if not ids:
            return []
        if context == 0:
            result = self.fetch_by_ids(ids)
            for msg in result:
                msg["in_source_ref"] = True
            return result

        id_set = set(ids)
        source = self.fetch_by_ids(ids)
        results: list[dict[str, Any]] = []
        seen: set[str] = set()
        with self._lock:
            for msg in source:
                rows = self._conn.execute(
                    """
                    SELECT id, session_key, seq, role, content, tool_chain, extra, ts
                    FROM messages
                    WHERE session_key = %s AND seq BETWEEN %s AND %s
                    ORDER BY seq ASC
                    """,
                    (
                        msg["session_key"],
                        max(0, int(msg["seq"]) - context),
                        int(msg["seq"]) + context,
                    ),
                ).fetchall()
                for row in rows:
                    item = self._row_to_message(row)
                    if item["id"] in seen:
                        continue
                    item["in_source_ref"] = item["id"] in id_set
                    seen.add(item["id"])
                    results.append(item)
        results.sort(key=lambda item: (str(item["session_key"]), int(item["seq"])))
        return results

    def search_messages(
        self,
        query: str,
        *,
        session_key: str | None = None,
        role: str | None = None,
        limit: int = 10,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        limit = max(1, min(int(limit), 100))
        offset = max(0, int(offset))
        terms = [term for term in query.split() if term] or [query]
        params: list[Any] = []
        where_parts: list[str] = []
        if session_key:
            where_parts.append("session_key = %s")
            params.append(session_key)
        if role:
            where_parts.append("role = %s")
            params.append(role)
        term_sql = " OR ".join("content ILIKE %s" for _ in terms)
        where_parts.append(f"({term_sql})")
        params.extend(f"%{term}%" for term in terms)
        where_sql = "WHERE " + " AND ".join(where_parts)
        with self._lock:
            count_row = self._conn.execute(
                f"SELECT COUNT(1) AS c FROM messages {where_sql}",
                tuple(params),
            ).fetchone()
            rows = self._conn.execute(
                f"""
                SELECT id, session_key, seq, role, content, tool_chain, extra, ts
                FROM messages
                {where_sql}
                ORDER BY ts DESC, seq DESC
                LIMIT %s OFFSET %s
                """,
                tuple([*params, limit, offset]),
            ).fetchall()
        total = int((count_row["c"] if count_row else 0) or 0)
        return [self._row_to_message(row) for row in rows], total

    def _row_to_message(self, row: dict[str, Any]) -> dict[str, Any]:
        message: dict[str, Any] = {
            "id": row["id"],
            "session_key": row["session_key"],
            "seq": int(row["seq"]),
            "role": row["role"],
            "content": row["content"] or "",
            "timestamp": row["ts"].isoformat() if hasattr(row["ts"], "isoformat") else row["ts"],
        }
        tool_chain = _json_value(row.get("tool_chain"), None)
        if tool_chain:
            message["tool_chain"] = tool_chain
        extra = _json_value(row.get("extra"), {})
        if isinstance(extra, dict) and extra:
            message.update(extra)
        return message
