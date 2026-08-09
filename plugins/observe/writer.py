"""Async TraceWriter for observe events.

Keeps the public API unchanged while allowing SQLite or PostgreSQL writes.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .db import open_db
from .events import MemoryWriteTrace, RagQueryLog, TurnTrace

try:
    import psycopg
    from psycopg.types.json import Jsonb
except ImportError:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]
    Jsonb = None  # type: ignore[assignment]

logger = logging.getLogger("observe.writer")

_QUEUE_MAX = 500
_ARG_MAX = 300
_RESULT_MAX = 500


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _serialize_tool_calls(tool_calls: list[dict]) -> str | None:
    if not tool_calls:
        return None
    slim = [
        {
            "name": c.get("name", ""),
            "args": str(c.get("args", c.get("arguments", "")))[:_ARG_MAX],
            "result": str(c.get("result", ""))[:_RESULT_MAX],
        }
        for c in tool_calls
    ]
    return json.dumps(slim, ensure_ascii=False)


def _split_session_key(session_key: str) -> tuple[str, str]:
    if ":" not in session_key:
        return "unknown", session_key
    channel, chat_id = session_key.split(":", 1)
    return channel or "unknown", chat_id or session_key


class TraceWriter:
    def __init__(self, db_path: Path, *, database_url: str | None = None) -> None:
        self._db_path = db_path
        self._database_url = (database_url or "").strip() or None
        self._queue: asyncio.Queue[
            TurnTrace | RagQueryLog | MemoryWriteTrace
        ] = asyncio.Queue(maxsize=_QUEUE_MAX)
        self._dropped = 0

    def emit(self, event: TurnTrace | RagQueryLog | MemoryWriteTrace) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._dropped += 1
            if self._dropped % 100 == 1:
                logger.warning("observe queue full, total_dropped=%d", self._dropped)

    async def drain(self) -> None:
        await self._queue.join()

    async def run(self) -> None:
        conn = _open_writer_db(self._db_path, self._database_url)
        logger.info("observe writer started: %s", self._database_url or self._db_path)
        try:
            while True:
                event = await self._queue.get()
                try:
                    self._write_one(conn, event)
                except Exception:
                    logger.exception("observe write failed for %s", type(event).__name__)
                finally:
                    self._queue.task_done()
        finally:
            while not self._queue.empty():
                try:
                    event = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                try:
                    self._write_one(conn, event)
                except Exception:
                    pass
                finally:
                    self._queue.task_done()
            conn.close()
            logger.info("observe writer stopped")

    def _write_one(
        self,
        conn: Any,
        event: TurnTrace | RagQueryLog | MemoryWriteTrace,
    ) -> None:
        ts = _now_iso()
        if isinstance(event, TurnTrace):
            _write_turn(conn, event, ts)
        elif isinstance(event, RagQueryLog):
            _write_rag(conn, event, ts)
        elif isinstance(event, MemoryWriteTrace):
            _write_memory_write(conn, event, ts)


def _open_writer_db(db_path: Path, database_url: str | None) -> Any:
    if database_url:
        if psycopg is None:
            raise RuntimeError("psycopg is required for postgres observe writer")
        return psycopg.connect(database_url)
    return open_db(db_path)


def _is_postgres_conn(conn: Any) -> bool:
    if psycopg is None:
        return False
    return isinstance(conn, psycopg.Connection)


def _pg_json(value: Any) -> Any:
    if Jsonb is None:
        return value
    return Jsonb(value)


def _ensure_postgres_session(conn: Any, session_key: str, ts: str) -> None:
    channel, chat_id = _split_session_key(session_key)
    conn.execute(
        """
        INSERT INTO sessions(
            key, user_id, channel, chat_id, created_at, updated_at,
            last_consolidated, metadata, next_seq
        )
        VALUES (%s, NULL, %s, %s, %s, %s, 0, %s, 0)
        ON CONFLICT(key) DO UPDATE SET
            updated_at = excluded.updated_at,
            channel = excluded.channel,
            chat_id = excluded.chat_id
        """,
        (session_key, channel, chat_id, ts, ts, _pg_json({})),
    )
    conn.commit()


def _write_turn(conn: Any, e: TurnTrace, ts: str) -> None:
    if _is_postgres_conn(conn):
        tool_calls_json = _serialize_tool_calls(e.tool_calls)
        _ensure_postgres_session(conn, e.session_key, ts)
        conn.execute(
            """
            INSERT INTO turns (
                ts, source, session_key, user_id, user_msg, llm_output,
                raw_llm_output, meme_tag, meme_media_count, tool_calls, tool_chain_json,
                history_window, history_messages, history_chars, history_tokens,
                prompt_tokens, next_turn_baseline_tokens, react_iteration_count,
                react_input_sum_tokens, react_input_peak_tokens, react_final_input_tokens,
                react_cache_prompt_tokens, react_cache_hit_tokens, error
            )
            VALUES (
                %s, %s, %s, NULL, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            """,
            (
                ts,
                e.source,
                e.session_key,
                e.user_msg,
                e.llm_output,
                e.raw_llm_output,
                e.meme_tag,
                e.meme_media_count,
                _pg_json(json.loads(tool_calls_json)) if tool_calls_json else None,
                _pg_json(json.loads(e.tool_chain_json)) if e.tool_chain_json else None,
                e.history_window,
                e.history_messages,
                e.history_chars,
                e.history_tokens,
                e.prompt_tokens,
                e.next_turn_baseline_tokens,
                e.react_iteration_count,
                e.react_input_sum_tokens,
                e.react_input_peak_tokens,
                e.react_final_input_tokens,
                e.react_cache_prompt_tokens,
                e.react_cache_hit_tokens,
                e.error,
            ),
        )
        conn.commit()
        return

    with conn:
        conn.execute(
            """
            INSERT INTO turns (
                ts, source, session_key, user_msg, llm_output,
                raw_llm_output, meme_tag, meme_media_count,
                tool_calls, tool_chain_json,
                history_window, history_messages, history_chars,
                history_tokens, prompt_tokens, next_turn_baseline_tokens,
                react_iteration_count, react_input_sum_tokens,
                react_input_peak_tokens, react_final_input_tokens,
                react_cache_prompt_tokens, react_cache_hit_tokens,
                error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                e.source,
                e.session_key,
                e.user_msg,
                e.llm_output,
                e.raw_llm_output,
                e.meme_tag,
                e.meme_media_count,
                _serialize_tool_calls(e.tool_calls),
                e.tool_chain_json,
                e.history_window,
                e.history_messages,
                e.history_chars,
                e.history_tokens,
                e.prompt_tokens,
                e.next_turn_baseline_tokens,
                e.react_iteration_count,
                e.react_input_sum_tokens,
                e.react_input_peak_tokens,
                e.react_final_input_tokens,
                e.react_cache_prompt_tokens,
                e.react_cache_hit_tokens,
                e.error,
            ),
        )


def _write_rag(conn: Any, e: RagQueryLog, ts: str) -> None:
    hits = [
        {
            "id": h.item_id,
            "type": h.memory_type,
            "score": round(h.score, 4),
            "summary": h.summary,
            "injected": h.injected,
        }
        for h in e.hits
    ]
    hits_json = hits or None
    aux_queries = e.aux_queries or None

    if _is_postgres_conn(conn):
        _ensure_postgres_session(conn, e.session_key, ts)
        conn.execute(
            """
            INSERT INTO rag_queries (
                ts, caller, session_key, user_id, query, orig_query,
                aux_queries, hits_json, injected_count, route_decision, error
            ) VALUES (%s, %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                ts,
                e.caller,
                e.session_key,
                e.query,
                e.orig_query,
                _pg_json(aux_queries) if aux_queries else None,
                _pg_json(hits_json) if hits_json else None,
                e.injected_count,
                e.route_decision,
                e.error,
            ),
        )
        conn.commit()
        return

    with conn:
        conn.execute(
            """
            INSERT INTO rag_queries (
                ts, caller, session_key, query, orig_query,
                aux_queries, hits_json, injected_count, route_decision, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                e.caller,
                e.session_key,
                e.query,
                e.orig_query,
                json.dumps(aux_queries, ensure_ascii=False) if aux_queries else None,
                json.dumps(hits_json, ensure_ascii=False) if hits_json else None,
                e.injected_count,
                e.route_decision,
                e.error,
            ),
        )


def _write_memory_write(conn: Any, e: MemoryWriteTrace, ts: str) -> None:
    superseded_ids = e.superseded_ids or None
    if _is_postgres_conn(conn):
        _ensure_postgres_session(conn, e.session_key, ts)
        conn.execute(
            """
            INSERT INTO memory_writes (
                ts, session_key, user_id, source_ref, action, memory_type,
                item_id, summary, superseded_ids, error
            )
            VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                ts,
                e.session_key,
                e.source_ref,
                e.action,
                e.memory_type,
                e.item_id,
                e.summary,
                _pg_json(superseded_ids) if superseded_ids else None,
                e.error,
            ),
        )
        conn.commit()
        return

    with conn:
        conn.execute(
            """
            INSERT INTO memory_writes (ts, session_key, source_ref, action, memory_type, item_id, summary, superseded_ids, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts,
                e.session_key,
                e.source_ref,
                e.action,
                e.memory_type,
                e.item_id,
                e.summary,
                json.dumps(superseded_ids, ensure_ascii=False) if superseded_ids else None,
                e.error,
            ),
        )
