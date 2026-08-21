from __future__ import annotations

import hashlib
import json
import logging
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from memory2.store import (
    VEC_DIM,
    _EmbeddingRow,
    _MemoryHit,
    _TIME_FILTER_KEYWORD_CANDIDATE_LIMIT,
    _TIME_FILTER_MARGIN,
    _coerce_emotional_weight,
    _coerce_float,
    _coerce_int,
    _content_hash,
    _cosine_similarity,
    _hotness_score,
    _is_memory_time_in_range,
    _json_object,
    _local_naive_iso,
    _now_iso,
    _parse_memory_time,
    _result_score,
)

logger = logging.getLogger(__name__)

_UUID_NAMESPACE = uuid.UUID("e7c31773-90fd-42a3-9e1d-2698aa6ec3d7")


def _deterministic_uuid(name: str) -> str:
    return str(uuid.uuid5(_UUID_NAMESPACE, name))


def _split_session_key(session_key: str) -> tuple[str, str]:
    if ":" not in session_key:
        return "unknown", session_key
    channel, chat_id = session_key.split(":", 1)
    return channel or "unknown", chat_id or session_key


def _workspace_user_id(workspace: Path) -> str | None:
    if workspace.parent.name == "users":
        candidate = workspace.name.strip()
        if candidate:
            return candidate
    return None


def _stable_workspace_user_id(workspace: Path) -> str:
    return _deterministic_uuid(f"workspace-user:{workspace.resolve()}")


def resolve_memory_user_id(
    *,
    workspace: Path,
    database_url: str,
    proactive_session_key: str = "",
) -> str:
    direct = _workspace_user_id(workspace)
    if direct:
        return direct

    dsn = database_url.replace("postgresql+psycopg://", "postgresql://", 1)
    conn = psycopg.connect(dsn, row_factory=dict_row)
    try:
        session_key = str(proactive_session_key or "").strip()
        if session_key:
            row = conn.execute(
                "SELECT user_id FROM sessions WHERE key = %s",
                (session_key,),
            ).fetchone()
            if row and row.get("user_id"):
                return str(row["user_id"])
        row = conn.execute(
            "SELECT id FROM users ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
        if row and row.get("id"):
            return str(row["id"])
    finally:
        conn.close()
    return _stable_workspace_user_id(workspace)


def _schema_sql() -> str:
    root = Path(__file__).resolve().parents[1]
    return (root / "scripts" / "memory2_pg_schema.sql").read_text(encoding="utf-8")


def _vector_literal(embedding: list[float] | None) -> str | None:
    if not embedding:
        return None
    return "[" + ",".join(format(float(v), ".12g") for v in embedding) + "]"


def _embedding_from_row(value: object) -> list[float] | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.startswith("[") and text.endswith("]"):
        payload = text[1:-1].strip()
        if not payload:
            return []
        return [float(part.strip()) for part in payload.split(",")]
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return [float(part) for part in data]
    except Exception:
        return None
    return None


def _json_dict(value: object) -> dict[str, object]:
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return cast(dict[str, object], value)
    try:
        data = json.loads(str(value))
    except Exception:
        return {}
    return cast(dict[str, object], data) if isinstance(data, dict) else {}


def _time_prefilter_clauses_pg(
    column: str,
    time_start: datetime | None,
    time_end: datetime | None,
) -> tuple[list[str], list[object]]:
    clauses = [f"{column} IS NOT NULL"]
    params: list[object] = []
    if time_start is not None:
        clauses.append(f"{column} >= %s")
        params.append(_local_naive_iso(time_start - _TIME_FILTER_MARGIN))
    if time_end is not None:
        clauses.append(f"{column} < %s")
        params.append(_local_naive_iso(time_end + _TIME_FILTER_MARGIN))
    return clauses, params


class PostgresMemoryStore:
    def __init__(
        self,
        database_url: str,
        *,
        workspace: Path,
        user_id: str,
        vec_dim: int = VEC_DIM,
    ) -> None:
        self.database_url = database_url
        self.workspace = workspace
        self.user_id = str(user_id).strip()
        self._vec_dim = int(vec_dim)
        self._lock = threading.RLock()
        dsn = database_url.replace("postgresql+psycopg://", "postgresql://", 1)
        self._conn = psycopg.connect(dsn, row_factory=dict_row)
        self._closed = False
        self._ensure_schema()
        self._ensure_user_row()

    @property
    def db_path(self) -> str:
        return self.database_url

    def _ensure_schema(self) -> None:
        self._conn.execute(_schema_sql())
        self._conn.commit()

    def _ensure_user_row(self) -> None:
        email = f"workspace-{self.user_id}@local.akashic"
        display_name = f"Workspace {self.workspace.name}"
        self._conn.execute(
            """
            INSERT INTO users(id, email, password_hash, display_name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT(id) DO NOTHING
            """,
            (self.user_id, email, "external:memory2-postgres", display_name),
        )
        self._conn.commit()

    def _rollback_if_needed(self) -> None:
        try:
            self._conn.rollback()
        except Exception:
            pass

    def _ensure_session(self, session_key: str, ts: str | None = None) -> None:
        clean = str(session_key or "").strip()
        if not clean:
            return
        channel, chat_id = _split_session_key(clean)
        now = ts or _now_iso()
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
            (clean, self.user_id, channel, chat_id, now, now, Jsonb({})),
        )

    def _infer_session_key(
        self,
        *,
        extra: dict[str, object] | None = None,
        source_ref: str | None = None,
    ) -> str | None:
        payload = extra or {}
        scope_channel = str(payload.get("scope_channel") or "").strip()
        scope_chat_id = str(payload.get("scope_chat_id") or "").strip()
        if scope_channel and scope_chat_id:
            return f"{scope_channel}:{scope_chat_id}"
        source = str(source_ref or "").strip()
        if source.startswith("telegram:") or source.startswith("web:"):
            return source
        return None

    def close(self) -> None:
        if self._closed:
            return
        try:
            self._conn.close()
        finally:
            self._closed = True

    def __del__(self) -> None:
        self.close()

    def upsert_item(
        self,
        memory_type: str,
        summary: str,
        embedding: list[float] | None,
        source_ref: str | None = None,
        extra: dict[str, object] | None = None,
        happened_at: str | None = None,
        emotional_weight: int = 0,
    ) -> str:
        chash = _content_hash(summary, memory_type)
        emotional_weight = _coerce_emotional_weight(emotional_weight)
        session_key = self._infer_session_key(extra=extra, source_ref=source_ref)
        with self._lock:
            existing = self._conn.execute(
                """
                SELECT id, status
                FROM memory_items
                WHERE user_id = %s AND content_hash = %s AND memory_type = %s
                """,
                (self.user_id, chash, memory_type),
            ).fetchone()
            if existing is not None:
                row_id = str(existing["id"])
                if str(existing["status"] or "") == "superseded":
                    self._conn.execute(
                        """
                        UPDATE memory_items
                        SET status = 'active',
                            reinforcement = reinforcement + 1,
                            updated_at = %s,
                            emotional_weight = GREATEST(emotional_weight, %s)
                        WHERE id = %s
                        """,
                        (_now_iso(), emotional_weight, row_id),
                    )
                else:
                    self._conn.execute(
                        """
                        UPDATE memory_items
                        SET reinforcement = reinforcement + 1,
                            updated_at = %s,
                            emotional_weight = GREATEST(emotional_weight, %s)
                        WHERE id = %s
                        """,
                        (_now_iso(), emotional_weight, row_id),
                    )
                self._conn.commit()
                return f"reinforced:{row_id}"

            item_id = hashlib.md5(f"{chash}{datetime.now().timestamp()}".encode()).hexdigest()[:12]
            if session_key:
                self._ensure_session(session_key, _now_iso())
            self._conn.execute(
                """
                INSERT INTO memory_items(
                    id, user_id, session_key, memory_type, summary, content_hash, embedding,
                    emotional_weight, extra_json, source_ref, happened_at, created_at, updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s::vector, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    item_id,
                    self.user_id,
                    session_key,
                    memory_type,
                    summary,
                    chash,
                    _vector_literal(embedding),
                    emotional_weight,
                    Jsonb(extra or {}),
                    source_ref,
                    happened_at,
                    _now_iso(),
                    _now_iso(),
                ),
            )
            self._conn.commit()
            return f"new:{item_id}"

    def upsert_consolidation_event(
        self,
        *,
        source_ref: str,
        summary: str,
        embedding: list[float] | None,
        extra: dict[str, object] | None = None,
        happened_at: str | None = None,
        emotional_weight: int = 0,
    ) -> str:
        src = (source_ref or "").strip()
        text = (summary or "").strip()
        if not src or not text:
            return "skipped:empty"
        emotional_weight = _coerce_emotional_weight(emotional_weight)
        session_key = self._infer_session_key(extra=extra, source_ref=source_ref)
        with self._lock:
            already = self._conn.execute(
                """
                SELECT item_id
                FROM consolidation_events
                WHERE user_id = %s AND source_ref = %s
                """,
                (self.user_id, src),
            ).fetchone()
            if already is not None:
                existing_id = str(already["item_id"] or "")
                return f"skipped:{existing_id or src}"

            chash = _content_hash(text, "event")
            existing = self._conn.execute(
                """
                SELECT id, status
                FROM memory_items
                WHERE user_id = %s AND content_hash = %s AND memory_type = 'event'
                """,
                (self.user_id, chash),
            ).fetchone()
            if existing is not None:
                row_id = str(existing["id"])
                self._conn.execute(
                    """
                    UPDATE memory_items
                    SET status = 'active',
                        reinforcement = reinforcement + 1,
                        updated_at = %s,
                        emotional_weight = GREATEST(emotional_weight, %s),
                        happened_at = COALESCE(NULLIF(happened_at::text, ''), %s::timestamptz)
                    WHERE id = %s
                    """,
                    (_now_iso(), emotional_weight, happened_at, row_id),
                )
                item_id = row_id
                result = f"reinforced:{row_id}"
            else:
                item_id = hashlib.md5(f"{chash}{datetime.now().timestamp()}".encode()).hexdigest()[:12]
                if session_key:
                    self._ensure_session(session_key, _now_iso())
                self._conn.execute(
                    """
                    INSERT INTO memory_items(
                        id, user_id, session_key, memory_type, summary, content_hash, embedding,
                        emotional_weight, extra_json, source_ref, happened_at, created_at, updated_at
                    )
                    VALUES (
                        %s, %s, %s, 'event', %s, %s, %s::vector, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        item_id,
                        self.user_id,
                        session_key,
                        text,
                        chash,
                        _vector_literal(embedding),
                        emotional_weight,
                        Jsonb(extra or {}),
                        src,
                        happened_at,
                        _now_iso(),
                        _now_iso(),
                    ),
                )
                result = f"new:{item_id}"

            self._conn.execute(
                """
                INSERT INTO consolidation_events(user_id, source_ref, item_id, session_key, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (self.user_id, src, item_id, session_key, _now_iso()),
            )
            self._conn.commit()
            return result

    def has_consolidation_source_ref(self, source_ref: str) -> bool:
        row = self._conn.execute(
            """
            SELECT 1
            FROM consolidation_events
            WHERE user_id = %s AND source_ref = %s
            LIMIT 1
            """,
            (self.user_id, (source_ref or "").strip()),
        ).fetchone()
        return row is not None

    def mark_superseded(self, item_id: str) -> None:
        self._conn.execute(
            "UPDATE memory_items SET status = 'superseded', updated_at = %s WHERE user_id = %s AND id = %s",
            (_now_iso(), self.user_id, item_id),
        )
        self._conn.commit()

    def mark_superseded_batch(self, ids: list[str]) -> None:
        if not ids:
            return
        self._conn.execute(
            "UPDATE memory_items SET status = 'superseded', updated_at = %s WHERE user_id = %s AND id = ANY(%s)",
            (_now_iso(), self.user_id, ids),
        )
        self._conn.commit()

    def get_items_by_ids(self, ids: list[str]) -> list[dict[str, object]]:
        if not ids:
            return []
        rows = self._conn.execute(
            """
            SELECT id, memory_type, summary, extra_json, source_ref, happened_at,
                   status, created_at, updated_at, emotional_weight
            FROM memory_items
            WHERE user_id = %s AND id = ANY(%s)
            """,
            (self.user_id, ids),
        ).fetchall()
        by_id: dict[str, dict[str, object]] = {}
        for row in rows:
            by_id[str(row["id"])] = {
                "id": str(row["id"]),
                "memory_type": str(row["memory_type"]),
                "summary": str(row["summary"]),
                "extra_json": _json_dict(row["extra_json"]),
                "source_ref": row["source_ref"],
                "happened_at": row["happened_at"].isoformat() if row["happened_at"] else None,
                "status": str(row["status"]),
                "created_at": row["created_at"].isoformat() if row["created_at"] else "",
                "updated_at": row["updated_at"].isoformat() if row["updated_at"] else "",
                "emotional_weight": _coerce_emotional_weight(row["emotional_weight"]),
            }
        return [by_id[item_id] for item_id in ids if item_id in by_id]

    def record_replacements(
        self,
        *,
        old_items: list[dict[str, object]],
        new_item: dict[str, object],
        source_ref: str | None = None,
        relation_type: str = "supersede",
    ) -> int:
        rows: list[tuple[object, ...]] = []
        session_key = self._infer_session_key(
            extra=cast(dict[str, object], new_item.get("extra_json") or {}),
            source_ref=cast(str | None, new_item.get("source_ref")),
        )
        for old_item in old_items:
            if not old_item.get("id") or not new_item.get("id"):
                continue
            rows.append(
                (
                    self.user_id,
                    session_key,
                    str(old_item.get("id")),
                    str(old_item.get("memory_type") or ""),
                    str(old_item.get("summary") or ""),
                    old_item.get("source_ref"),
                    old_item.get("happened_at"),
                    Jsonb(old_item.get("extra_json") or {}),
                    str(new_item.get("id")),
                    str(new_item.get("memory_type") or ""),
                    str(new_item.get("summary") or ""),
                    new_item.get("source_ref"),
                    new_item.get("happened_at"),
                    Jsonb(new_item.get("extra_json") or {}),
                    relation_type,
                    source_ref or new_item.get("source_ref"),
                    _now_iso(),
                )
            )
        if not rows:
            return 0
        self._conn.executemany(
            """
            INSERT INTO memory_replacements(
                user_id, session_key, old_item_id, old_memory_type, old_summary, old_source_ref,
                old_happened_at, old_extra_json, new_item_id, new_memory_type, new_summary,
                new_source_ref, new_happened_at, new_extra_json, relation_type, source_ref, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )
        self._conn.commit()
        return len(rows)

    def list_replacements(self) -> list[dict]:
        rows = self._conn.execute(
            """
            SELECT old_item_id, old_memory_type, old_summary, old_source_ref, old_happened_at,
                   old_extra_json, new_item_id, new_memory_type, new_summary, new_source_ref,
                   new_happened_at, new_extra_json, relation_type, source_ref, created_at
            FROM memory_replacements
            WHERE user_id = %s
            ORDER BY id ASC
            """,
            (self.user_id,),
        ).fetchall()
        results: list[dict[str, object]] = []
        for row in rows:
            results.append(
                {
                    "old_item_id": row["old_item_id"],
                    "old_memory_type": row["old_memory_type"],
                    "old_summary": row["old_summary"],
                    "old_source_ref": row["old_source_ref"],
                    "old_happened_at": row["old_happened_at"].isoformat() if row["old_happened_at"] else None,
                    "old_extra_json": _json_dict(row["old_extra_json"]),
                    "new_item_id": row["new_item_id"],
                    "new_memory_type": row["new_memory_type"],
                    "new_summary": row["new_summary"],
                    "new_source_ref": row["new_source_ref"],
                    "new_happened_at": row["new_happened_at"].isoformat() if row["new_happened_at"] else None,
                    "new_extra_json": _json_dict(row["new_extra_json"]),
                    "relation_type": row["relation_type"],
                    "source_ref": row["source_ref"],
                    "created_at": row["created_at"].isoformat() if row["created_at"] else "",
                }
            )
        return results

    def reinforce_items_batch(self, ids: list[str], emotional_weight: int = 0) -> None:
        if not ids:
            return
        self._conn.execute(
            """
            UPDATE memory_items
            SET reinforcement = reinforcement + 1,
                updated_at = %s,
                emotional_weight = GREATEST(emotional_weight, %s)
            WHERE user_id = %s AND id = ANY(%s)
            """,
            (_now_iso(), _coerce_emotional_weight(emotional_weight), self.user_id, ids),
        )
        self._conn.commit()

    def list_items_for_dashboard(
        self,
        *,
        q: str = "",
        memory_type: str = "",
        status: str = "",
        source_ref: str = "",
        scope_channel: str = "",
        scope_chat_id: str = "",
        has_embedding: bool | None = None,
        page: int = 1,
        page_size: int = 50,
        sort_by: str = "created_at",
        sort_order: str = "desc",
    ) -> tuple[list[dict[str, object]], int]:
        safe_sort_by = sort_by if sort_by in {
            "updated_at", "created_at", "happened_at", "reinforcement",
            "emotional_weight", "memory_type",
        } else "created_at"
        safe_sort_order = "ASC" if sort_order == "asc" else "DESC"
        safe_page = max(1, page)
        safe_page_size = max(1, min(page_size, 200))
        offset = (safe_page - 1) * safe_page_size
        where_parts = ["user_id = %s"]
        params: list[object] = [self.user_id]
        if q:
            like = f"%{q}%"
            where_parts.append("(id ILIKE %s OR summary ILIKE %s OR COALESCE(source_ref, '') ILIKE %s)")
            params.extend([like, like, like])
        if memory_type:
            where_parts.append("memory_type = %s")
            params.append(memory_type)
        if status:
            where_parts.append("status = %s")
            params.append(status)
        if source_ref:
            where_parts.append("COALESCE(source_ref, '') ILIKE %s")
            params.append(f"%{source_ref}%")
        if scope_channel:
            where_parts.append("COALESCE(extra_json->>'scope_channel', '') = %s")
            params.append(scope_channel)
        if scope_chat_id:
            where_parts.append("COALESCE(extra_json->>'scope_chat_id', '') = %s")
            params.append(scope_chat_id)
        if has_embedding is True:
            where_parts.append("embedding IS NOT NULL")
        if has_embedding is False:
            where_parts.append("embedding IS NULL")
        where_sql = " AND ".join(where_parts)
        total = int(
            self._conn.execute(
                f"SELECT COUNT(*) AS c FROM memory_items WHERE {where_sql}",
                tuple(params),
            ).fetchone()["c"]
        )
        rows = self._conn.execute(
            f"""
            SELECT id, memory_type, summary, source_ref, happened_at, status,
                   created_at, updated_at, reinforcement, emotional_weight, extra_json,
                   (embedding IS NOT NULL) AS has_embedding
            FROM memory_items
            WHERE {where_sql}
            ORDER BY {safe_sort_by} {safe_sort_order}, id ASC
            LIMIT %s OFFSET %s
            """,
            tuple([*params, safe_page_size, offset]),
        ).fetchall()
        items: list[dict[str, object]] = []
        for row in rows:
            extra = _json_dict(row["extra_json"])
            items.append(
                {
                    "id": str(row["id"]),
                    "memory_type": str(row["memory_type"]),
                    "summary": str(row["summary"]),
                    "source_ref": row["source_ref"],
                    "happened_at": row["happened_at"].isoformat() if row["happened_at"] else None,
                    "status": str(row["status"]),
                    "created_at": row["created_at"].isoformat() if row["created_at"] else "",
                    "updated_at": row["updated_at"].isoformat() if row["updated_at"] else "",
                    "reinforcement": _coerce_int(row["reinforcement"], 1),
                    "emotional_weight": _coerce_emotional_weight(row["emotional_weight"]),
                    "has_embedding": bool(row["has_embedding"]),
                    "scope_channel": extra.get("scope_channel", ""),
                    "scope_chat_id": extra.get("scope_chat_id", ""),
                }
            )
        return items, total

    def get_item_for_dashboard(
        self,
        item_id: str,
        *,
        include_embedding: bool = False,
    ) -> dict[str, object] | None:
        row = self._conn.execute(
            """
            SELECT id, memory_type, summary, content_hash, embedding, reinforcement, emotional_weight,
                   extra_json, source_ref, happened_at, status, created_at, updated_at
            FROM memory_items
            WHERE user_id = %s AND id = %s
            """,
            (self.user_id, item_id),
        ).fetchone()
        if row is None:
            return None
        embedding = _embedding_from_row(row["embedding"])
        return {
            "id": str(row["id"]),
            "memory_type": str(row["memory_type"]),
            "summary": str(row["summary"]),
            "content_hash": str(row["content_hash"]),
            "reinforcement": _coerce_int(row["reinforcement"], 1),
            "emotional_weight": _coerce_emotional_weight(row["emotional_weight"]),
            "extra_json": _json_dict(row["extra_json"]),
            "source_ref": row["source_ref"],
            "happened_at": row["happened_at"].isoformat() if row["happened_at"] else None,
            "status": str(row["status"]),
            "created_at": row["created_at"].isoformat() if row["created_at"] else "",
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else "",
            "has_embedding": embedding is not None,
            "embedding_dim": len(embedding) if embedding is not None else 0,
            "embedding": embedding if include_embedding else None,
        }

    def update_item_for_dashboard(
        self,
        item_id: str,
        *,
        status: str | None = None,
        extra_json: dict[str, object] | None = None,
        source_ref: str | None = None,
        happened_at: str | None = None,
        emotional_weight: int | None = None,
    ) -> dict[str, object] | None:
        updates: list[str] = []
        params: list[object] = []
        if status is not None:
            safe_status = status.strip()
            if safe_status not in {"active", "superseded"}:
                raise ValueError("status only supports active or superseded")
            updates.append("status = %s")
            params.append(safe_status)
        if extra_json is not None:
            updates.append("extra_json = %s")
            params.append(Jsonb(extra_json))
        if source_ref is not None:
            updates.append("source_ref = %s")
            params.append(source_ref)
        if happened_at is not None:
            updates.append("happened_at = %s")
            params.append(happened_at)
        if emotional_weight is not None:
            updates.append("emotional_weight = %s")
            params.append(_coerce_emotional_weight(emotional_weight))
        if not updates:
            return self.get_item_for_dashboard(item_id)
        updates.append("updated_at = %s")
        params.append(_now_iso())
        params.extend([self.user_id, item_id])
        cur = self._conn.execute(
            f"UPDATE memory_items SET {', '.join(updates)} WHERE user_id = %s AND id = %s",
            tuple(params),
        )
        self._conn.commit()
        if cur.rowcount <= 0:
            return None
        return self.get_item_for_dashboard(item_id)

    def delete_item(self, item_id: str) -> bool:
        cur = self._conn.execute(
            "DELETE FROM memory_items WHERE user_id = %s AND id = %s",
            (self.user_id, item_id),
        )
        self._conn.commit()
        return bool(cur.rowcount and cur.rowcount > 0)

    def delete_items_batch(self, ids: list[str]) -> int:
        if not ids:
            return 0
        cur = self._conn.execute(
            "DELETE FROM memory_items WHERE user_id = %s AND id = ANY(%s)",
            (self.user_id, ids),
        )
        self._conn.commit()
        return int(cur.rowcount or 0)

    def find_similar_items_for_dashboard(
        self,
        item_id: str,
        *,
        top_k: int = 8,
        memory_type: str = "",
        score_threshold: float = 0.0,
        include_superseded: bool = False,
    ) -> list[dict[str, object]]:
        base = self.get_item_for_dashboard(item_id, include_embedding=True)
        if base is None:
            raise KeyError(item_id)
        embedding = base.get("embedding")
        if not isinstance(embedding, list) or not embedding:
            raise ValueError("memory has no embedding")
        results = self.vector_search(
            query_vec=embedding,
            top_k=max(1, top_k) + 1,
            memory_types=[memory_type] if memory_type else None,
            score_threshold=score_threshold,
            include_superseded=include_superseded,
        )
        filtered = [item for item in results if item.get("id") != item_id]
        return filtered[: max(1, top_k)]

    def get_all_with_embedding(self, include_superseded: bool = False) -> list[_EmbeddingRow]:
        where_parts = ["user_id = %s", "embedding IS NOT NULL"]
        params: list[object] = [self.user_id]
        if not include_superseded:
            where_parts.append("status = 'active'")
        rows = self._conn.execute(
            f"""
            SELECT id, memory_type, summary, embedding, extra_json, happened_at,
                   reinforcement, updated_at, source_ref, emotional_weight
            FROM memory_items
            WHERE {' AND '.join(where_parts)}
            """,
            tuple(params),
        ).fetchall()
        result: list[_EmbeddingRow] = []
        for row in rows:
            emb = _embedding_from_row(row["embedding"])
            extra = _json_dict(row["extra_json"])
            extra["_reinforcement"] = _coerce_int(row["reinforcement"], 1)
            extra["_updated_at"] = row["updated_at"].isoformat() if row["updated_at"] else ""
            extra["_emotional_weight"] = _coerce_emotional_weight(row["emotional_weight"])
            result.append(
                (
                    str(row["id"]),
                    str(row["memory_type"]),
                    str(row["summary"]),
                    emb,
                    extra,
                    row["happened_at"].isoformat() if row["happened_at"] else None,
                    str(row["source_ref"]) if row["source_ref"] else None,
                )
            )
        return result

    def _get_embedding_rows_by_time_filter(
        self,
        *,
        memory_types: list[str] | None,
        include_superseded: bool,
        scope_channel: str | None,
        scope_chat_id: str | None,
        require_scope_match: bool,
        time_start: datetime | None,
        time_end: datetime | None,
    ) -> list[_EmbeddingRow]:
        where_parts = ["user_id = %s", "embedding IS NOT NULL"]
        params: list[object] = [self.user_id]
        if not include_superseded:
            where_parts.append("status = 'active'")
        if memory_types:
            where_parts.append("memory_type = ANY(%s)")
            params.append(memory_types)
        if require_scope_match:
            where_parts.append("COALESCE(extra_json->>'scope_channel', '') = %s")
            where_parts.append("COALESCE(extra_json->>'scope_chat_id', '') = %s")
            params.extend([(scope_channel or "").strip(), (scope_chat_id or "").strip()])
        time_clauses, time_params = _time_prefilter_clauses_pg("happened_at::text", time_start, time_end)
        where_parts.extend(time_clauses)
        params.extend(time_params)
        rows = self._conn.execute(
            f"""
            SELECT id, memory_type, summary, embedding, extra_json, happened_at,
                   reinforcement, updated_at, source_ref, emotional_weight
            FROM memory_items
            WHERE {' AND '.join(where_parts)}
            """,
            tuple(params),
        ).fetchall()
        result: list[_EmbeddingRow] = []
        for row in rows:
            happened_at = row["happened_at"].isoformat() if row["happened_at"] else None
            if not _is_memory_time_in_range(happened_at, time_start, time_end):
                continue
            emb = _embedding_from_row(row["embedding"])
            extra = _json_dict(row["extra_json"])
            extra["_reinforcement"] = _coerce_int(row["reinforcement"], 1)
            extra["_updated_at"] = row["updated_at"].isoformat() if row["updated_at"] else ""
            extra["_emotional_weight"] = _coerce_emotional_weight(row["emotional_weight"])
            result.append(
                (
                    str(row["id"]),
                    str(row["memory_type"]),
                    str(row["summary"]),
                    emb,
                    extra,
                    happened_at,
                    str(row["source_ref"]) if row["source_ref"] else None,
                )
            )
        return result

    def vector_search(
        self,
        query_vec: list[float],
        top_k: int = 8,
        memory_types: list[str] | None = None,
        score_threshold: float = 0.0,
        include_superseded: bool = False,
        scope_channel: str | None = None,
        scope_chat_id: str | None = None,
        require_scope_match: bool = False,
        hotness_alpha: float = 0.0,
        hotness_half_life_days: float = 14.0,
        time_start: datetime | None = None,
        time_end: datetime | None = None,
    ) -> list[dict[str, object]]:
        if time_start is not None or time_end is not None:
            return self._vector_search_fullscan(
                query_vec,
                top_k=top_k,
                memory_types=memory_types,
                score_threshold=score_threshold,
                include_superseded=include_superseded,
                scope_channel=scope_channel,
                scope_chat_id=scope_chat_id,
                require_scope_match=require_scope_match,
                hotness_alpha=hotness_alpha,
                hotness_half_life_days=hotness_half_life_days,
                time_start=time_start,
                time_end=time_end,
            )
        return self._vector_search_pg(
            query_vec,
            top_k=top_k,
            memory_types=memory_types,
            score_threshold=score_threshold,
            include_superseded=include_superseded,
            scope_channel=scope_channel,
            scope_chat_id=scope_chat_id,
            require_scope_match=require_scope_match,
            hotness_alpha=hotness_alpha,
            hotness_half_life_days=hotness_half_life_days,
        )

    def vector_search_batch(
        self,
        query_vecs: list[list[float]],
        top_k: int = 8,
        memory_types: list[str] | None = None,
        score_threshold: float = 0.0,
        include_superseded: bool = False,
        scope_channel: str | None = None,
        scope_chat_id: str | None = None,
        require_scope_match: bool = False,
        hotness_alpha: float = 0.0,
        hotness_half_life_days: float = 14.0,
        time_start: datetime | None = None,
        time_end: datetime | None = None,
    ) -> list[list[dict[str, object]]]:
        if not query_vecs:
            return []
        if time_start is not None or time_end is not None:
            rows = self._get_embedding_rows_by_time_filter(
                memory_types=memory_types,
                include_superseded=include_superseded,
                scope_channel=scope_channel,
                scope_chat_id=scope_chat_id,
                require_scope_match=require_scope_match,
                time_start=time_start,
                time_end=time_end,
            )
            return [
                self._score_embedding_rows(
                    query_vec,
                    rows,
                    top_k=top_k,
                    score_threshold=score_threshold,
                    hotness_alpha=hotness_alpha,
                    hotness_half_life_days=hotness_half_life_days,
                )
                for query_vec in query_vecs
            ]
        return [
            self.vector_search(
                query_vec,
                top_k=top_k,
                memory_types=memory_types,
                score_threshold=score_threshold,
                include_superseded=include_superseded,
                scope_channel=scope_channel,
                scope_chat_id=scope_chat_id,
                require_scope_match=require_scope_match,
                hotness_alpha=hotness_alpha,
                hotness_half_life_days=hotness_half_life_days,
            )
            for query_vec in query_vecs
        ]

    def _vector_search_pg(
        self,
        query_vec: list[float],
        *,
        top_k: int,
        memory_types: list[str] | None,
        score_threshold: float,
        include_superseded: bool,
        scope_channel: str | None,
        scope_chat_id: str | None,
        require_scope_match: bool,
        hotness_alpha: float,
        hotness_half_life_days: float,
    ) -> list[_MemoryHit]:
        where_parts = ["user_id = %s", "embedding IS NOT NULL"]
        params: list[object] = [self.user_id]
        if not include_superseded:
            where_parts.append("status = 'active'")
        if memory_types:
            where_parts.append("memory_type = ANY(%s)")
            params.append(memory_types)
        if require_scope_match:
            where_parts.append("COALESCE(extra_json->>'scope_channel', '') = %s")
            where_parts.append("COALESCE(extra_json->>'scope_chat_id', '') = %s")
            params.extend([(scope_channel or "").strip(), (scope_chat_id or "").strip()])
        query_vector = _vector_literal(query_vec)
        limit_value = max(top_k * 2, 20)
        sql_params = [query_vector, *params, query_vector, limit_value]
        try:
            rows = self._conn.execute(
                f"""
                SELECT id, memory_type, summary, extra_json, happened_at, reinforcement,
                       updated_at, source_ref, emotional_weight, 1 - (embedding <=> %s::vector) AS similarity
                FROM memory_items
                WHERE {' AND '.join(where_parts)}
                ORDER BY embedding <=> %s::vector ASC
                LIMIT %s
                """,
                tuple(sql_params),
            ).fetchall()
        except Exception:
            self._rollback_if_needed()
            raise
        now = datetime.now(timezone.utc)
        scored: list[_MemoryHit] = []
        for row in rows:
            similarity = _coerce_float(row["similarity"])
            if similarity < score_threshold:
                continue
            reinforcement = _coerce_int(row["reinforcement"], 1)
            updated_at_str = row["updated_at"].isoformat() if row["updated_at"] else ""
            emotional_weight = _coerce_emotional_weight(row["emotional_weight"])
            extra = _json_dict(row["extra_json"])
            extra["_reinforcement"] = reinforcement
            extra["_updated_at"] = updated_at_str
            extra["_emotional_weight"] = emotional_weight
            hotness = 0.0
            if hotness_alpha > 0 and updated_at_str:
                try:
                    hotness = _hotness_score(
                        reinforcement,
                        datetime.fromisoformat(updated_at_str),
                        now,
                        hotness_half_life_days,
                        emotional_weight=emotional_weight,
                    )
                except Exception:
                    pass
            final = (1.0 - hotness_alpha) * similarity + hotness_alpha * hotness
            scored.append(
                {
                    "id": str(row["id"]),
                    "memory_type": str(row["memory_type"]),
                    "summary": str(row["summary"]),
                    "extra_json": extra,
                    "happened_at": row["happened_at"].isoformat() if row["happened_at"] else "",
                    "source_ref": str(row["source_ref"]) if row["source_ref"] else "",
                    "score": round(final, 4),
                    "_score_debug": {
                        "semantic": round(similarity, 4),
                        "hotness": round(hotness, 4),
                        "final": round(final, 4),
                    },
                }
            )
        scored.sort(key=_result_score, reverse=True)
        return scored[:top_k]

    def _vector_search_fullscan(
        self,
        query_vec: list[float],
        *,
        top_k: int,
        memory_types: list[str] | None,
        score_threshold: float,
        include_superseded: bool,
        scope_channel: str | None,
        scope_chat_id: str | None,
        require_scope_match: bool,
        hotness_alpha: float,
        hotness_half_life_days: float,
        time_start: datetime | None = None,
        time_end: datetime | None = None,
    ) -> list[_MemoryHit]:
        rows = (
            self._get_embedding_rows_by_time_filter(
                memory_types=memory_types,
                include_superseded=include_superseded,
                scope_channel=scope_channel,
                scope_chat_id=scope_chat_id,
                require_scope_match=require_scope_match,
                time_start=time_start,
                time_end=time_end,
            )
            if (time_start is not None or time_end is not None)
            else self.get_all_with_embedding(include_superseded=include_superseded)
        )
        if memory_types and time_start is None and time_end is None:
            rows = [row for row in rows if row[1] in memory_types]
        if require_scope_match and time_start is None and time_end is None:
            s_channel = (scope_channel or "").strip()
            s_chat = (scope_chat_id or "").strip()
            rows = [
                row for row in rows
                if str((row[4] or {}).get("scope_channel", "")).strip() == s_channel
                and str((row[4] or {}).get("scope_chat_id", "")).strip() == s_chat
            ]
        return self._score_embedding_rows(
            query_vec,
            rows,
            top_k=top_k,
            score_threshold=score_threshold,
            hotness_alpha=hotness_alpha,
            hotness_half_life_days=hotness_half_life_days,
        )

    def _score_embedding_rows(
        self,
        query_vec: list[float],
        rows: list[_EmbeddingRow],
        *,
        top_k: int,
        score_threshold: float,
        hotness_alpha: float,
        hotness_half_life_days: float,
    ) -> list[dict[str, object]]:
        if not rows:
            return []
        scored: list[_MemoryHit] = []
        now = datetime.now(timezone.utc)
        for row_id, mtype, summary, emb, extra, happened_at, source_ref in rows:
            if emb is None:
                continue
            semantic = _cosine_similarity(query_vec, emb)
            if semantic < score_threshold:
                continue
            hotness = 0.0
            if hotness_alpha > 0:
                reinforcement = _coerce_int(extra.get("_reinforcement"), 1)
                updated_at_str = str(extra.get("_updated_at") or "")
                emotional_weight = _coerce_emotional_weight(extra.get("_emotional_weight", 0))
                if updated_at_str:
                    try:
                        hotness = _hotness_score(
                            reinforcement,
                            datetime.fromisoformat(updated_at_str),
                            now,
                            hotness_half_life_days,
                            emotional_weight=emotional_weight,
                        )
                    except Exception:
                        pass
            final = (1.0 - hotness_alpha) * semantic + hotness_alpha * hotness
            scored.append(
                {
                    "id": row_id,
                    "memory_type": mtype,
                    "summary": summary,
                    "extra_json": extra,
                    "happened_at": happened_at or "",
                    "source_ref": source_ref or "",
                    "score": round(final, 4),
                    "_score_debug": {
                        "semantic": round(semantic, 4),
                        "hotness": round(hotness, 4),
                        "final": round(final, 4),
                    },
                }
            )
        scored.sort(key=_result_score, reverse=True)
        return scored[:top_k]

    def merge_item_raw(
        self,
        item_id: str,
        new_summary: str,
        new_hash: str,
        new_embedding: list[float],
        new_extra: dict[str, object] | None = None,
    ) -> None:
        updates = [
            "summary = %s",
            "content_hash = %s",
            "embedding = %s::vector",
            "reinforcement = reinforcement + 1",
            "updated_at = %s",
        ]
        params: list[object] = [new_summary, new_hash, _vector_literal(new_embedding), _now_iso()]
        if new_extra is not None:
            updates.append("extra_json = %s")
            params.append(Jsonb(new_extra))
        params.extend([self.user_id, item_id])
        try:
            self._conn.execute(
                f"UPDATE memory_items SET {', '.join(updates)} WHERE user_id = %s AND id = %s",
                tuple(params),
            )
            self._conn.commit()
        except psycopg.IntegrityError:
            self._conn.rollback()
            row = self._conn.execute(
                "SELECT memory_type FROM memory_items WHERE user_id = %s AND id = %s",
                (self.user_id, item_id),
            ).fetchone()
            if row:
                self.mark_superseded(item_id)
                self.upsert_item(
                    memory_type=str(row["memory_type"]),
                    summary=new_summary,
                    embedding=new_embedding,
                )

    def list_by_type(self, memory_type: str) -> list[dict[str, object]]:
        rows = self._conn.execute(
            """
            SELECT id, memory_type, summary, extra_json, happened_at, reinforcement, emotional_weight
            FROM memory_items
            WHERE user_id = %s AND memory_type = %s
            """,
            (self.user_id, memory_type),
        ).fetchall()
        return [
            {
                "id": str(row["id"]),
                "memory_type": str(row["memory_type"]),
                "summary": str(row["summary"]),
                "extra_json": _json_dict(row["extra_json"]),
                "happened_at": row["happened_at"].isoformat() if row["happened_at"] else None,
                "reinforcement": _coerce_int(row["reinforcement"], 1),
                "emotional_weight": _coerce_emotional_weight(row["emotional_weight"]),
            }
            for row in rows
        ]

    def list_events_by_time_range(
        self,
        time_start: datetime,
        time_end: datetime,
        limit: int = 200,
    ) -> list[dict[str, object]]:
        rows = self._conn.execute(
            """
            SELECT id, memory_type, summary, source_ref, happened_at
            FROM memory_items
            WHERE user_id = %s
              AND memory_type = 'event'
              AND status = 'active'
              AND happened_at IS NOT NULL
              AND happened_at >= %s
              AND happened_at < %s
            ORDER BY happened_at DESC
            LIMIT %s
            """,
            (self.user_id, time_start, time_end, max(1, min(limit, 200))),
        ).fetchall()
        hits = [
            (
                cast(datetime, row["happened_at"]),
                {
                    "id": str(row["id"]),
                    "memory_type": str(row["memory_type"]),
                    "summary": str(row["summary"]),
                    "source_ref": str(row["source_ref"]) if row["source_ref"] else "",
                    "happened_at": row["happened_at"].isoformat() if row["happened_at"] else "",
                    "score": 1.0,
                },
            )
            for row in rows
        ]
        hits.sort(key=lambda item: item[0])
        return [item for _, item in hits]

    def find_similar_recent_events(
        self,
        embedding: list[float],
        *,
        days_back: int = 7,
        threshold: float = 0.92,
        top_k: int = 3,
    ) -> list[str]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=max(1, int(days_back)))
        rows = self.vector_search(
            query_vec=embedding,
            top_k=max(1, int(top_k)) * 3,
            memory_types=["event"],
            score_threshold=float(threshold),
            time_start=cutoff,
            time_end=datetime.now(timezone.utc),
        )
        return [str(row["id"]) for row in rows[: max(1, int(top_k))]]

    def delete_by_source_ref(self, source_ref: str) -> int:
        cur = self._conn.execute(
            "DELETE FROM memory_items WHERE user_id = %s AND source_ref = %s",
            (self.user_id, source_ref),
        )
        self._conn.commit()
        return int(cur.rowcount or 0)

    def has_item_by_source_ref(
        self,
        source_ref: str,
        memory_type: str | None = None,
    ) -> bool:
        if memory_type:
            row = self._conn.execute(
                """
                SELECT 1 FROM memory_items
                WHERE user_id = %s AND source_ref = %s AND memory_type = %s
                LIMIT 1
                """,
                (self.user_id, source_ref, memory_type),
            ).fetchone()
        else:
            row = self._conn.execute(
                """
                SELECT 1 FROM memory_items
                WHERE user_id = %s AND source_ref = %s
                LIMIT 1
                """,
                (self.user_id, source_ref),
            ).fetchone()
        return row is not None

    def keyword_match_procedures(self, action_tokens: list[str]) -> list[dict[str, object]]:
        if not action_tokens:
            return []
        token_set = {t.lower() for t in action_tokens if t}
        action_text = " ".join(action_tokens).lower()
        rows = self._conn.execute(
            """
            SELECT id, summary, extra_json
            FROM memory_items
            WHERE user_id = %s
              AND memory_type = 'procedure'
              AND status = 'active'
              AND extra_json IS NOT NULL
            """,
            (self.user_id,),
        ).fetchall()
        matched: list[dict[str, object]] = []
        for row in rows:
            extra = _json_dict(row["extra_json"])
            tags = cast(dict[str, object], extra.get("trigger_tags") or {})
            if tags.get("scope") != "tool_triggered":
                continue
            keywords = [k for k in cast(list[str], tags.get("keywords") or []) if k and len(k) >= 3]
            if keywords:
                hit = any(str(kw).lower() in action_text for kw in keywords)
            else:
                proc_tools = cast(list[str], tags.get("tools") or [])
                proc_skills = cast(list[str], tags.get("skills") or [])
                if len(proc_tools) > 4:
                    continue
                tag_token_set = {t.lower() for t in proc_tools}
                tag_token_set |= {s.lower() for s in proc_skills}
                hit = bool(token_set & tag_token_set)
            if hit:
                matched.append(
                    {
                        "id": str(row["id"]),
                        "memory_type": "procedure",
                        "summary": str(row["summary"]),
                        "extra_json": extra,
                        "intercept": bool(tags.get("intercept", False)),
                        "score": 1.0,
                    }
                )
        return matched

    def keyword_search_summary(
        self,
        terms: list[str],
        memory_types: list[str] | None = None,
        limit: int = 20,
        time_start: datetime | None = None,
        time_end: datetime | None = None,
        scope_channel: str | None = None,
        scope_chat_id: str | None = None,
        require_scope_match: bool = False,
    ) -> list[dict[str, object]]:
        terms = [t for t in terms if t and len(t) >= 2]
        if not terms:
            return []
        where_parts = ["user_id = %s", "status = 'active'"]
        params: list[object] = [self.user_id]
        if memory_types:
            where_parts.append("memory_type = ANY(%s)")
            params.append(memory_types)
        if require_scope_match:
            where_parts.append("COALESCE(extra_json->>'scope_channel', '') = %s")
            where_parts.append("COALESCE(extra_json->>'scope_chat_id', '') = %s")
            params.extend([(scope_channel or "").strip(), (scope_chat_id or "").strip()])
        has_time_filter = time_start is not None or time_end is not None
        if has_time_filter:
            time_clauses, time_params = _time_prefilter_clauses_pg("happened_at::text", time_start, time_end)
            where_parts.extend(time_clauses)
            params.extend(time_params)
        or_conditions = " OR ".join("summary ILIKE %s" for _ in terms)
        score_expr = " + ".join("(CASE WHEN summary ILIKE %s THEN 1 ELSE 0 END)" for _ in terms)
        like_vals = [f"%{t}%" for t in terms]
        batch_size = max(limit, _TIME_FILTER_KEYWORD_CANDIDATE_LIMIT) if has_time_filter else limit
        sql = (
            f"SELECT id, memory_type, summary, source_ref, happened_at, created_at, reinforcement, "
            f"({score_expr}) AS kw_score "
            f"FROM memory_items WHERE {' AND '.join(where_parts)} AND ({or_conditions}) "
            f"ORDER BY kw_score DESC, reinforcement DESC, id ASC LIMIT %s OFFSET %s"
        )
        results: list[dict[str, object]] = []
        offset = 0
        while True:
            try:
                rows = self._conn.execute(
                    sql,
                    tuple(like_vals + params + like_vals + [batch_size, offset]),
                ).fetchall()
            except Exception:
                self._rollback_if_needed()
                raise
            if not rows:
                break
            for row in rows:
                happened_at = row["happened_at"].isoformat() if row["happened_at"] else None
                if has_time_filter and not _is_memory_time_in_range(happened_at, time_start, time_end):
                    continue
                results.append(
                    {
                        "id": str(row["id"]),
                        "memory_type": str(row["memory_type"]),
                        "summary": str(row["summary"]),
                        "source_ref": str(row["source_ref"]) if row["source_ref"] else "",
                        "happened_at": happened_at or (row["created_at"].isoformat() if row["created_at"] else ""),
                        "keyword_score": _coerce_float(row["kw_score"]) / len(terms),
                    }
                )
                if len(results) >= limit:
                    return results
            if not has_time_filter or len(rows) < batch_size:
                break
            offset += batch_size
        return results
