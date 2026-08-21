from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

try:
    import psycopg
    from psycopg.types.json import Jsonb
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency: psycopg. Install with `python -m pip install psycopg[binary]`."
    ) from exc


DEFAULT_DATABASE_URL = "postgresql://akashic:akashic@localhost:5432/akashic_agent"
DEFAULT_SESSION_KEY = "telegram:8759439816"
DEFAULT_USER_EMAIL = "telegram-8759439816@local.akashic"
UUID_NAMESPACE = uuid.UUID("e7c31773-90fd-42a3-9e1d-2698aa6ec3d7")


def deterministic_uuid(name: str) -> str:
    return str(uuid.uuid5(UUID_NAMESPACE, name))


def split_session_key(session_key: str) -> tuple[str, str]:
    if ":" not in session_key:
        return "unknown", session_key
    channel, chat_id = session_key.split(":", 1)
    return channel or "unknown", chat_id or session_key


def parse_json(raw: Any, fallback: Any) -> Any:
    if raw is None or raw == "":
        return fallback
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(str(raw))
    except Exception:
        return fallback


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


@contextmanager
def readable_sqlite_path(db_path: Path, *, copy_first: bool = False):
    if not copy_first:
        yield db_path
        return
    if not db_path.exists():
        yield db_path
        return
    with tempfile.TemporaryDirectory(prefix="akashic-sqlite-") as tmp:
        copied = Path(tmp) / db_path.name
        shutil.copy2(db_path, copied)
        yield copied


def sqlite_rows(db_path: Path, table: str, *, copy_first: bool = False) -> list[sqlite3.Row]:
    if not db_path.exists():
        return []
    with readable_sqlite_path(db_path, copy_first=copy_first) as readable:
        return _sqlite_rows_from_path(readable, table)


def _sqlite_rows_from_path(db_path: Path, table: str) -> list[sqlite3.Row]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        if not table_exists(conn, table):
            return []
        return list(conn.execute(f"SELECT * FROM {table}"))
    finally:
        conn.close()


def ensure_schema(pg: psycopg.Connection[Any], schema_path: Path) -> None:
    pg.execute(schema_path.read_text(encoding="utf-8"))
    pg.commit()


def ensure_user(
    pg: psycopg.Connection[Any],
    *,
    user_id: str,
    email: str,
    display_name: str,
) -> None:
    pg.execute(
        """
        INSERT INTO users(id, email, password_hash, display_name)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT(id) DO UPDATE SET
            email = excluded.email,
            display_name = excluded.display_name
        """,
        (user_id, email, "external:telegram", display_name),
    )


def ensure_session(
    pg: psycopg.Connection[Any],
    *,
    session_key: str,
    user_id: str | None,
    created_at: str,
    updated_at: str,
    last_consolidated: int = 0,
    metadata: dict[str, Any] | None = None,
    last_user_at: str | None = None,
    last_proactive_at: str | None = None,
    next_seq: int = 0,
) -> None:
    channel, chat_id = split_session_key(session_key)
    pg.execute(
        """
        INSERT INTO sessions(
            key, user_id, channel, chat_id, created_at, updated_at,
            last_consolidated, metadata, last_user_at, last_proactive_at, next_seq
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(key) DO UPDATE SET
            user_id = COALESCE(excluded.user_id, sessions.user_id),
            channel = excluded.channel,
            chat_id = excluded.chat_id,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            last_consolidated = excluded.last_consolidated,
            metadata = excluded.metadata,
            last_user_at = excluded.last_user_at,
            last_proactive_at = excluded.last_proactive_at,
            next_seq = GREATEST(sessions.next_seq, excluded.next_seq)
        """,
        (
            session_key,
            user_id,
            channel,
            chat_id,
            created_at,
            updated_at,
            int(last_consolidated or 0),
            Jsonb(metadata or {}),
            last_user_at,
            last_proactive_at,
            int(next_seq or 0),
        ),
    )


def ensure_conversation(
    pg: psycopg.Connection[Any],
    *,
    user_id: str,
    session_key: str,
    title: str,
    created_at: str,
    updated_at: str,
) -> str:
    conversation_id = deterministic_uuid(f"conversation:{session_key}")
    pg.execute(
        """
        INSERT INTO conversations(id, user_id, session_key, title, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT(session_key) DO UPDATE SET
            user_id = excluded.user_id,
            title = excluded.title,
            updated_at = excluded.updated_at
        """,
        (conversation_id, user_id, session_key, title, created_at, updated_at),
    )
    return conversation_id


def ensure_runtime_session_for_row(
    pg: psycopg.Connection[Any],
    *,
    session_key: str,
    target_session_key: str,
    default_user_id: str,
    attach_all_sessions: bool,
    timestamp: str,
) -> str | None:
    user_id = default_user_id if attach_all_sessions or session_key == target_session_key else None
    ensure_session(
        pg,
        session_key=session_key,
        user_id=user_id,
        created_at=timestamp,
        updated_at=timestamp,
    )
    if user_id is not None:
        ensure_conversation(
            pg,
            user_id=user_id,
            session_key=session_key,
            title=session_key,
            created_at=timestamp,
            updated_at=timestamp,
        )
    return user_id


def migrate_sessions(
    pg: psycopg.Connection[Any],
    *,
    workspace: Path,
    target_session_key: str,
    default_user_id: str,
    attach_all_sessions: bool,
) -> int:
    rows = sqlite_rows(workspace / "sessions.db", "sessions")
    migrated = 0
    for row in rows:
        session_key = str(row["key"])
        user_id = default_user_id if attach_all_sessions or session_key == target_session_key else None
        metadata = parse_json(row["metadata"], {})
        ensure_session(
            pg,
            session_key=session_key,
            user_id=user_id,
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            last_consolidated=int(row["last_consolidated"] or 0),
            metadata=metadata,
            last_user_at=row["last_user_at"] if "last_user_at" in row.keys() else None,
            last_proactive_at=row["last_proactive_at"] if "last_proactive_at" in row.keys() else None,
            next_seq=int(row["next_seq"] or 0) if "next_seq" in row.keys() else 0,
        )
        if user_id is not None:
            ensure_conversation(
                pg,
                user_id=user_id,
                session_key=session_key,
                title=session_key,
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
            )
        migrated += 1

    if not rows:
        ensure_session(
            pg,
            session_key=target_session_key,
            user_id=default_user_id,
            created_at="1970-01-01T00:00:00+00:00",
            updated_at="1970-01-01T00:00:00+00:00",
        )
        ensure_conversation(
            pg,
            user_id=default_user_id,
            session_key=target_session_key,
            title=target_session_key,
            created_at="1970-01-01T00:00:00+00:00",
            updated_at="1970-01-01T00:00:00+00:00",
        )
        migrated = 1
    return migrated


def migrate_messages(
    pg: psycopg.Connection[Any],
    *,
    workspace: Path,
    target_session_key: str,
    default_user_id: str,
    attach_all_sessions: bool,
) -> int:
    rows = sqlite_rows(workspace / "sessions.db", "messages")
    for row in rows:
        session_key = str(row["session_key"])
        user_id = ensure_runtime_session_for_row(
            pg,
            session_key=session_key,
            target_session_key=target_session_key,
            default_user_id=default_user_id,
            attach_all_sessions=attach_all_sessions,
            timestamp=str(row["ts"]),
        )
        pg.execute(
            """
            INSERT INTO messages(id, session_key, user_id, seq, role, content, tool_chain, extra, ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(id) DO UPDATE SET
                session_key = excluded.session_key,
                user_id = excluded.user_id,
                seq = excluded.seq,
                role = excluded.role,
                content = excluded.content,
                tool_chain = excluded.tool_chain,
                extra = excluded.extra,
                ts = excluded.ts
            """,
            (
                row["id"],
                session_key,
                user_id,
                int(row["seq"]),
                row["role"],
                row["content"],
                Jsonb(parse_json(row["tool_chain"], None)) if row["tool_chain"] else None,
                Jsonb(parse_json(row["extra"], {})),
                row["ts"],
            ),
        )
    return len(rows)


def migrate_proactive(
    pg: psycopg.Connection[Any],
    *,
    workspace: Path,
    target_session_key: str,
    default_user_id: str,
    attach_all_sessions: bool,
) -> dict[str, int]:
    db_path = workspace / "proactive.db"
    counts: dict[str, int] = {}
    for row in sqlite_rows(db_path, "seen_items", copy_first=True):
        pg.execute(
            """
            INSERT INTO seen_items(source_key, item_id, seen_at)
            VALUES (%s, %s, %s)
            ON CONFLICT(source_key, item_id) DO UPDATE SET seen_at = excluded.seen_at
            """,
            (row["source_key"], row["item_id"], row["seen_at"]),
        )
        counts["seen_items"] = counts.get("seen_items", 0) + 1

    for row in sqlite_rows(db_path, "deliveries", copy_first=True):
        ensure_runtime_session_for_row(
            pg,
            session_key=str(row["session_key"]),
            target_session_key=target_session_key,
            default_user_id=default_user_id,
            attach_all_sessions=attach_all_sessions,
            timestamp=str(row["sent_at"]),
        )
        pg.execute(
            """
            INSERT INTO deliveries(session_key, delivery_key, sent_at)
            VALUES (%s, %s, %s)
            ON CONFLICT(session_key, delivery_key) DO UPDATE SET sent_at = excluded.sent_at
            """,
            (row["session_key"], row["delivery_key"], row["sent_at"]),
        )
        counts["deliveries"] = counts.get("deliveries", 0) + 1

    for row in sqlite_rows(db_path, "rejection_cooldown", copy_first=True):
        pg.execute(
            """
            INSERT INTO rejection_cooldown(source_key, item_id, rejected_at)
            VALUES (%s, %s, %s)
            ON CONFLICT(source_key, item_id) DO UPDATE SET rejected_at = excluded.rejected_at
            """,
            (row["source_key"], row["item_id"], row["rejected_at"]),
        )
        counts["rejection_cooldown"] = counts.get("rejection_cooldown", 0) + 1

    for row in sqlite_rows(db_path, "semantic_items", copy_first=True):
        pg.execute(
            """
            INSERT INTO semantic_items(source_key, item_id, text, ts)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT(source_key, item_id, ts) DO UPDATE SET text = excluded.text
            """,
            (row["source_key"], row["item_id"], row["text"], row["ts"]),
        )
        counts["semantic_items"] = counts.get("semantic_items", 0) + 1

    for row in sqlite_rows(db_path, "kv_state", copy_first=True):
        pg.execute(
            """
            INSERT INTO kv_state(key, value)
            VALUES (%s, %s)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (row["key"], row["value"]),
        )
        counts["kv_state"] = counts.get("kv_state", 0) + 1

    for row in sqlite_rows(db_path, "session_state", copy_first=True):
        ensure_runtime_session_for_row(
            pg,
            session_key=str(row["session_key"]),
            target_session_key=target_session_key,
            default_user_id=default_user_id,
            attach_all_sessions=attach_all_sessions,
            timestamp="1970-01-01T00:00:00+00:00",
        )
        pg.execute(
            """
            INSERT INTO session_state(session_key, key, value)
            VALUES (%s, %s, %s)
            ON CONFLICT(session_key, key) DO UPDATE SET value = excluded.value
            """,
            (row["session_key"], row["key"], row["value"]),
        )
        counts["session_state"] = counts.get("session_state", 0) + 1

    for row in sqlite_rows(db_path, "context_only_timestamps", copy_first=True):
        ensure_runtime_session_for_row(
            pg,
            session_key=str(row["session_key"]),
            target_session_key=target_session_key,
            default_user_id=default_user_id,
            attach_all_sessions=attach_all_sessions,
            timestamp=str(row["ts"]),
        )
        pg.execute(
            """
            INSERT INTO context_only_timestamps(session_key, ts)
            VALUES (%s, %s)
            ON CONFLICT(session_key, ts) DO NOTHING
            """,
            (row["session_key"], row["ts"]),
        )
        counts["context_only_timestamps"] = counts.get("context_only_timestamps", 0) + 1

    for row in sqlite_rows(db_path, "tick_log", copy_first=True):
        session_key = str(row["session_key"])
        user_id = ensure_runtime_session_for_row(
            pg,
            session_key=session_key,
            target_session_key=target_session_key,
            default_user_id=default_user_id,
            attach_all_sessions=attach_all_sessions,
            timestamp=str(row["started_at"]),
        )
        pg.execute(
            """
            INSERT INTO tick_log(
                tick_id, session_key, user_id, started_at, finished_at, gate_exit,
                terminal_action, skip_reason, steps_taken, alert_count, content_count,
                context_count, interesting_ids, discarded_ids, cited_ids, drift_entered,
                final_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(tick_id) DO UPDATE SET
                session_key = excluded.session_key,
                user_id = excluded.user_id,
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
                row["tick_id"],
                session_key,
                user_id,
                row["started_at"],
                row["finished_at"],
                row["gate_exit"],
                row["terminal_action"],
                row["skip_reason"],
                row["steps_taken"],
                row["alert_count"],
                row["content_count"],
                row["context_count"],
                Jsonb(parse_json(row["interesting_ids"], [])),
                Jsonb(parse_json(row["discarded_ids"], [])),
                Jsonb(parse_json(row["cited_ids"], [])),
                bool(row["drift_entered"]),
                row["final_message"],
            ),
        )
        counts["tick_log"] = counts.get("tick_log", 0) + 1

    for row in sqlite_rows(db_path, "tick_step_log", copy_first=True):
        pg.execute(
            """
            INSERT INTO tick_step_log(
                tick_id, step_index, phase, tool_name, tool_call_id, tool_args_json,
                tool_result_text, terminal_action_after, skip_reason_after,
                interesting_ids_after, discarded_ids_after, cited_ids_after,
                final_message_after
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                row["tick_id"],
                int(row["step_index"]),
                row["phase"],
                row["tool_name"],
                row["tool_call_id"],
                Jsonb(parse_json(row["tool_args_json"], {})),
                row["tool_result_text"],
                row["terminal_action_after"],
                row["skip_reason_after"],
                Jsonb(parse_json(row["interesting_ids_after"], [])),
                Jsonb(parse_json(row["discarded_ids_after"], [])),
                Jsonb(parse_json(row["cited_ids_after"], [])),
                row["final_message_after"],
            ),
        )
        counts["tick_step_log"] = counts.get("tick_step_log", 0) + 1
    return counts


def migrate_consolidation(pg: psycopg.Connection[Any], *, workspace: Path, target_session_key: str) -> int:
    rows = sqlite_rows(workspace / "memory" / "consolidation_writes.db", "consolidation_writes")
    for row in rows:
        pg.execute(
            """
            INSERT INTO consolidation_writes(
                source_ref, kind, session_key, payload, trailing_blank_line, done_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(source_ref, kind) DO UPDATE SET
                session_key = COALESCE(excluded.session_key, consolidation_writes.session_key),
                payload = excluded.payload,
                trailing_blank_line = excluded.trailing_blank_line,
                done_at = excluded.done_at
            """,
            (
                row["source_ref"],
                row["kind"],
                target_session_key,
                row["payload"] if "payload" in row.keys() else None,
                bool(row["trailing_blank_line"]) if "trailing_blank_line" in row.keys() else False,
                row["done_at"],
            ),
        )
    return len(rows)


def migrate_observe(
    pg: psycopg.Connection[Any],
    *,
    workspace: Path,
    target_session_key: str,
    default_user_id: str,
    attach_all_sessions: bool,
) -> dict[str, int]:
    db_path = workspace / "observe" / "observe.db"
    counts: dict[str, int] = {}
    for row in sqlite_rows(db_path, "turns"):
        session_key = str(row["session_key"])
        user_id = ensure_runtime_session_for_row(
            pg,
            session_key=session_key,
            target_session_key=target_session_key,
            default_user_id=default_user_id,
            attach_all_sessions=attach_all_sessions,
            timestamp=str(row["ts"]),
        )
        pg.execute(
            """
            INSERT INTO turns(
                id, ts, source, session_key, user_id, user_msg, llm_output, raw_llm_output,
                meme_tag, meme_media_count, tool_calls, tool_chain_json, history_window,
                history_messages, history_chars, history_tokens, prompt_tokens,
                next_turn_baseline_tokens, react_iteration_count, react_input_sum_tokens,
                react_input_peak_tokens, react_final_input_tokens, react_cache_prompt_tokens,
                react_cache_hit_tokens, error
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(id) DO UPDATE SET
                ts = excluded.ts,
                source = excluded.source,
                session_key = excluded.session_key,
                user_id = excluded.user_id,
                user_msg = excluded.user_msg,
                llm_output = excluded.llm_output,
                raw_llm_output = excluded.raw_llm_output,
                tool_calls = excluded.tool_calls,
                tool_chain_json = excluded.tool_chain_json,
                error = excluded.error
            """,
            (
                row["id"],
                row["ts"],
                row["source"],
                session_key,
                user_id,
                row["user_msg"],
                row["llm_output"],
                row["raw_llm_output"] if "raw_llm_output" in row.keys() else None,
                row["meme_tag"] if "meme_tag" in row.keys() else None,
                row["meme_media_count"] if "meme_media_count" in row.keys() else None,
                Jsonb(parse_json(row["tool_calls"], None)) if row["tool_calls"] else None,
                Jsonb(parse_json(row["tool_chain_json"], None)) if "tool_chain_json" in row.keys() and row["tool_chain_json"] else None,
                row["history_window"] if "history_window" in row.keys() else None,
                row["history_messages"] if "history_messages" in row.keys() else None,
                row["history_chars"] if "history_chars" in row.keys() else None,
                row["history_tokens"] if "history_tokens" in row.keys() else None,
                row["prompt_tokens"] if "prompt_tokens" in row.keys() else None,
                row["next_turn_baseline_tokens"] if "next_turn_baseline_tokens" in row.keys() else None,
                row["react_iteration_count"] if "react_iteration_count" in row.keys() else None,
                row["react_input_sum_tokens"] if "react_input_sum_tokens" in row.keys() else None,
                row["react_input_peak_tokens"] if "react_input_peak_tokens" in row.keys() else None,
                row["react_final_input_tokens"] if "react_final_input_tokens" in row.keys() else None,
                row["react_cache_prompt_tokens"] if "react_cache_prompt_tokens" in row.keys() else None,
                row["react_cache_hit_tokens"] if "react_cache_hit_tokens" in row.keys() else None,
                row["error"],
            ),
        )
        counts["turns"] = counts.get("turns", 0) + 1

    for row in sqlite_rows(db_path, "rag_queries"):
        session_key = str(row["session_key"])
        user_id = ensure_runtime_session_for_row(
            pg,
            session_key=session_key,
            target_session_key=target_session_key,
            default_user_id=default_user_id,
            attach_all_sessions=attach_all_sessions,
            timestamp=str(row["ts"]),
        )
        pg.execute(
            """
            INSERT INTO rag_queries(
                id, ts, caller, session_key, user_id, query, orig_query, aux_queries,
                hits_json, injected_count, route_decision, error
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(id) DO UPDATE SET
                ts = excluded.ts,
                caller = excluded.caller,
                session_key = excluded.session_key,
                user_id = excluded.user_id,
                query = excluded.query,
                orig_query = excluded.orig_query,
                aux_queries = excluded.aux_queries,
                hits_json = excluded.hits_json,
                injected_count = excluded.injected_count,
                route_decision = excluded.route_decision,
                error = excluded.error
            """,
            (
                row["id"],
                row["ts"],
                row["caller"],
                session_key,
                user_id,
                row["query"],
                row["orig_query"],
                Jsonb(parse_json(row["aux_queries"], None)) if row["aux_queries"] else None,
                Jsonb(parse_json(row["hits_json"], None)) if row["hits_json"] else None,
                int(row["injected_count"] or 0),
                row["route_decision"],
                row["error"],
            ),
        )
        counts["rag_queries"] = counts.get("rag_queries", 0) + 1

    for row in sqlite_rows(db_path, "memory_writes"):
        session_key = str(row["session_key"])
        user_id = ensure_runtime_session_for_row(
            pg,
            session_key=session_key,
            target_session_key=target_session_key,
            default_user_id=default_user_id,
            attach_all_sessions=attach_all_sessions,
            timestamp=str(row["ts"]),
        )
        pg.execute(
            """
            INSERT INTO memory_writes(
                id, ts, session_key, user_id, source_ref, action, memory_type,
                item_id, summary, superseded_ids, error
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(id) DO UPDATE SET
                ts = excluded.ts,
                session_key = excluded.session_key,
                user_id = excluded.user_id,
                source_ref = excluded.source_ref,
                action = excluded.action,
                memory_type = excluded.memory_type,
                item_id = excluded.item_id,
                summary = excluded.summary,
                superseded_ids = excluded.superseded_ids,
                error = excluded.error
            """,
            (
                row["id"],
                row["ts"],
                session_key,
                user_id,
                row["source_ref"],
                row["action"],
                row["memory_type"],
                row["item_id"],
                row["summary"],
                Jsonb(parse_json(row["superseded_ids"], None)) if row["superseded_ids"] else None,
                row["error"],
            ),
        )
        counts["memory_writes"] = counts.get("memory_writes", 0) + 1
    return counts


def migrate_schedules(
    pg: psycopg.Connection[Any],
    *,
    workspace: Path,
    target_session_key: str,
    default_user_id: str,
) -> int:
    path = workspace / "schedules.json"
    if not path.exists():
        return 0
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return 0
    if not isinstance(payload, list):
        return 0
    count = 0
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            continue
        session_key = str(item.get("session_key") or target_session_key)
        user_id = ensure_runtime_session_for_row(
            pg,
            session_key=session_key,
            target_session_key=target_session_key,
            default_user_id=default_user_id,
            attach_all_sessions=False,
            timestamp="1970-01-01T00:00:00+00:00",
        )
        schedule_id = deterministic_uuid(f"schedule:{session_key}:{index}:{json.dumps(item, sort_keys=True, ensure_ascii=False)}")
        pg.execute(
            """
            INSERT INTO schedules(id, user_id, session_key, name, spec_json, enabled)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(id) DO UPDATE SET
                user_id = excluded.user_id,
                session_key = excluded.session_key,
                name = excluded.name,
                spec_json = excluded.spec_json,
                enabled = excluded.enabled,
                updated_at = now()
            """,
            (
                schedule_id,
                user_id,
                session_key,
                str(item.get("name") or item.get("title") or f"schedule-{index}"),
                Jsonb(item),
                bool(item.get("enabled", True)),
            ),
        )
        count += 1
    return count


def migrate_webapp(pg: psycopg.Connection[Any], *, workspace: Path) -> dict[str, int]:
    db_path = workspace / "webapp.db"
    counts: dict[str, int] = {}
    user_rows = sqlite_rows(db_path, "users")
    for row in user_rows:
        pg.execute(
            """
            INSERT INTO users(id, email, password_hash, display_name, disabled, created_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(id) DO UPDATE SET
                email = excluded.email,
                password_hash = excluded.password_hash,
                display_name = excluded.display_name,
                disabled = excluded.disabled
            """,
            (
                row["id"],
                row["email"],
                row["password_hash"],
                row["display_name"],
                bool(row["disabled"]),
                row["created_at"],
            ),
        )
        counts["users"] = counts.get("users", 0) + 1

    for row in sqlite_rows(db_path, "conversations"):
        row_keys = set(row.keys())
        session_key = (
            str(row["session_key"])
            if "session_key" in row_keys and row["session_key"]
            else f"web:{row['user_id']}:{row['id']}"
        )
        ensure_session(
            pg,
            session_key=session_key,
            user_id=str(row["user_id"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )
        pg.execute(
            """
            INSERT INTO conversations(id, user_id, session_key, title, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT(id) DO UPDATE SET
                user_id = excluded.user_id,
                session_key = excluded.session_key,
                title = excluded.title,
                updated_at = excluded.updated_at
            """,
            (
                row["id"],
                row["user_id"],
                session_key,
                row["title"],
                row["created_at"],
                row["updated_at"],
            ),
        )
        pg.execute(
            "UPDATE sessions SET user_id = %s WHERE key = %s",
            (row["user_id"], session_key),
        )
        pg.execute(
            "UPDATE messages SET user_id = %s WHERE session_key = %s",
            (row["user_id"], session_key),
        )
        counts["conversations"] = counts.get("conversations", 0) + 1

    conversation_sessions = {}
    for row in sqlite_rows(db_path, "conversations"):
        row_keys = set(row.keys())
        conversation_sessions[str(row["id"])] = (
            str(row["session_key"])
            if "session_key" in row_keys and row["session_key"]
            else f"web:{row['user_id']}:{row['id']}"
        )
    for row in sqlite_rows(db_path, "chat_messages"):
        conversation_id = str(row["conversation_id"])
        session_key = conversation_sessions.get(conversation_id)
        if not session_key:
            session_key = f"web:{row['user_id']}:{conversation_id}"
        exists = pg.execute(
            """
            SELECT 1
            FROM messages
            WHERE session_key = %s AND role = %s AND content = %s AND ts = %s
            LIMIT 1
            """,
            (session_key, row["role"], row["content"], row["created_at"]),
        ).fetchone()
        if exists:
            continue
        seq_row = pg.execute(
            """
            SELECT GREATEST(
                COALESCE((SELECT next_seq FROM sessions WHERE key = %s), 0),
                COALESCE((SELECT MAX(seq) + 1 FROM messages WHERE session_key = %s), 0)
            )
            """,
            (session_key, session_key),
        ).fetchone()
        seq = int((seq_row[0] if seq_row else 0) or 0)
        message_id = f"{session_key}:webapp:{row['id']}"
        pg.execute(
            """
            INSERT INTO messages(id, session_key, user_id, seq, role, content, tool_chain, extra, ts)
            VALUES (%s, %s, %s, %s, %s, %s, NULL, %s, %s)
            ON CONFLICT(id) DO NOTHING
            """,
            (
                message_id,
                session_key,
                row["user_id"],
                seq,
                row["role"],
                row["content"],
                Jsonb(parse_json(row["metadata_json"], {})),
                row["created_at"],
            ),
        )
        pg.execute(
            """
            UPDATE sessions
            SET next_seq = GREATEST(next_seq, %s), updated_at = GREATEST(updated_at, %s)
            WHERE key = %s
            """,
            (seq + 1, row["created_at"], session_key),
        )
        counts["messages_from_chat_messages"] = counts.get("messages_from_chat_messages", 0) + 1

    for row in sqlite_rows(db_path, "agent_turns"):
        session_key = f"web:{row['user_id']}:{row['conversation_id']}"
        pg.execute(
            """
            INSERT INTO agent_turns(
                id, conversation_id, user_id, session_key, status, error, created_at, completed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(id) DO UPDATE SET
                conversation_id = excluded.conversation_id,
                user_id = excluded.user_id,
                session_key = excluded.session_key,
                status = excluded.status,
                error = excluded.error,
                completed_at = excluded.completed_at
            """,
            (
                row["id"],
                row["conversation_id"],
                row["user_id"],
                session_key,
                row["status"],
                row["error"],
                row["created_at"],
                row["completed_at"],
            ),
        )
        counts["agent_turns"] = counts.get("agent_turns", 0) + 1

    for row in sqlite_rows(db_path, "proactive_sessions"):
        pg.execute(
            """
            INSERT INTO proactive_sessions(
                id, user_id, conversation_id, session_key, enabled,
                last_tick_at, next_tick_at, interval_seconds, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(id) DO UPDATE SET
                user_id = excluded.user_id,
                conversation_id = excluded.conversation_id,
                session_key = excluded.session_key,
                enabled = excluded.enabled,
                last_tick_at = excluded.last_tick_at,
                next_tick_at = excluded.next_tick_at,
                interval_seconds = excluded.interval_seconds,
                updated_at = excluded.updated_at
            """,
            (
                row["id"],
                row["user_id"],
                row["conversation_id"],
                row["session_key"],
                bool(row["enabled"]),
                row["last_tick_at"],
                row["next_tick_at"],
                row["interval_seconds"],
                row["created_at"],
                row["updated_at"],
            ),
        )
        counts["proactive_sessions"] = counts.get("proactive_sessions", 0) + 1
    return counts


def print_counts(title: str, counts: int | dict[str, int]) -> None:
    if isinstance(counts, int):
        print(f"{title}: {counts}")
        return
    print(title + ":")
    for key in sorted(counts):
        print(f"  {key}: {counts[key]}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate Akashic SQLite state to PostgreSQL.")
    parser.add_argument("--workspace", type=Path, default=Path.home() / ".akashic" / "workspace")
    parser.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    parser.add_argument("--schema", type=Path, default=Path(__file__).with_name("postgres_schema.sql"))
    parser.add_argument("--session-key", default=DEFAULT_SESSION_KEY)
    parser.add_argument("--user-email", default=DEFAULT_USER_EMAIL)
    parser.add_argument("--display-name", default="Telegram 8759439816")
    parser.add_argument(
        "--attach-all-sessions",
        action="store_true",
        help="Attach every migrated session to the default user. By default only --session-key is attached.",
    )
    parser.add_argument("--skip-schema", action="store_true")
    args = parser.parse_args()

    user_id = deterministic_uuid(f"user:{args.user_email.lower()}")
    with psycopg.connect(args.database_url) as pg:
        if not args.skip_schema:
            ensure_schema(pg, args.schema)
        ensure_user(pg, user_id=user_id, email=args.user_email, display_name=args.display_name)

        session_count = migrate_sessions(
            pg,
            workspace=args.workspace,
            target_session_key=args.session_key,
            default_user_id=user_id,
            attach_all_sessions=args.attach_all_sessions,
        )
        message_count = migrate_messages(
            pg,
            workspace=args.workspace,
            target_session_key=args.session_key,
            default_user_id=user_id,
            attach_all_sessions=args.attach_all_sessions,
        )
        proactive_counts = migrate_proactive(
            pg,
            workspace=args.workspace,
            target_session_key=args.session_key,
            default_user_id=user_id,
            attach_all_sessions=args.attach_all_sessions,
        )
        consolidation_count = migrate_consolidation(
            pg,
            workspace=args.workspace,
            target_session_key=args.session_key,
        )
        observe_counts = migrate_observe(
            pg,
            workspace=args.workspace,
            target_session_key=args.session_key,
            default_user_id=user_id,
            attach_all_sessions=args.attach_all_sessions,
        )
        schedule_count = migrate_schedules(
            pg,
            workspace=args.workspace,
            target_session_key=args.session_key,
            default_user_id=user_id,
        )
        webapp_counts = migrate_webapp(pg, workspace=args.workspace)
        pg.commit()

    print(f"user_id: {user_id}")
    print_counts("sessions", session_count)
    print_counts("messages", message_count)
    print_counts("proactive", proactive_counts)
    print_counts("consolidation_writes", consolidation_count)
    print_counts("observe", observe_counts)
    print_counts("schedules", schedule_count)
    print_counts("webapp", webapp_counts)
    print("skipped: memory/memory2.db, memory_items.embedding, vec_items")


if __name__ == "__main__":
    main()
