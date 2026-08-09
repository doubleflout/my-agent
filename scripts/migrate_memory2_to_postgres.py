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


DEFAULT_DATABASE_URL = "postgresql://postgres:postgres123@localhost:5432/akashic_agent"
DEFAULT_USER_EMAIL = "telegram-8759439816@local.akashic"
DEFAULT_SESSION_KEY = "telegram:8759439816"
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
    with tempfile.TemporaryDirectory(prefix="akashic-memory2-") as tmp:
        copied = Path(tmp) / db_path.name
        shutil.copy2(db_path, copied)
        yield copied


def sqlite_rows(db_path: Path, table: str, *, copy_first: bool = False) -> list[sqlite3.Row]:
    if not db_path.exists():
        return []
    with readable_sqlite_path(db_path, copy_first=copy_first) as readable:
        conn = sqlite3.connect(str(readable))
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
        (user_id, email, "external:memory2-migration", display_name),
    )


def ensure_session(
    pg: psycopg.Connection[Any],
    *,
    session_key: str,
    user_id: str,
    created_at: str,
    updated_at: str,
) -> None:
    channel, chat_id = split_session_key(session_key)
    pg.execute(
        """
        INSERT INTO sessions(
            key, user_id, channel, chat_id, created_at, updated_at,
            last_consolidated, metadata, last_user_at, last_proactive_at, next_seq
        )
        VALUES (%s, %s, %s, %s, %s, %s, 0, %s, NULL, NULL, 0)
        ON CONFLICT(key) DO UPDATE SET
            user_id = COALESCE(sessions.user_id, excluded.user_id),
            channel = excluded.channel,
            chat_id = excluded.chat_id
        """,
        (
            session_key,
            user_id,
            channel,
            chat_id,
            created_at,
            updated_at,
            Jsonb({}),
        ),
    )


def infer_session_key(
    *,
    extra_json: dict[str, Any],
    source_ref: str | None,
    fallback_session_key: str | None,
) -> str | None:
    scope_channel = str(extra_json.get("scope_channel") or "").strip()
    scope_chat_id = str(extra_json.get("scope_chat_id") or "").strip()
    if scope_channel and scope_chat_id:
        return f"{scope_channel}:{scope_chat_id}"
    source = str(source_ref or "").strip()
    if source.startswith("telegram:") or source.startswith("web:"):
        return source
    return fallback_session_key


def vector_literal(embedding: list[float] | None) -> str | None:
    if not embedding:
        return None
    return "[" + ",".join(format(float(value), ".12g") for value in embedding) + "]"


def migrate_memory_items(
    pg: psycopg.Connection[Any],
    *,
    db_path: Path,
    user_id: str,
    fallback_session_key: str | None,
) -> int:
    rows = sqlite_rows(db_path, "memory_items", copy_first=True)
    count = 0
    for row in rows:
        extra_json = parse_json(row["extra_json"], {})
        embedding = parse_json(row["embedding"], None)
        session_key = infer_session_key(
            extra_json=extra_json,
            source_ref=row["source_ref"] if "source_ref" in row.keys() else None,
            fallback_session_key=fallback_session_key,
        )
        if session_key:
            ensure_session(
                pg,
                session_key=session_key,
                user_id=user_id,
                created_at=str(row["created_at"]),
                updated_at=str(row["updated_at"]),
            )
        pg.execute(
            """
            INSERT INTO memory_items(
                id, user_id, session_key, memory_type, summary, content_hash, embedding,
                reinforcement, emotional_weight, extra_json, source_ref, happened_at,
                status, created_at, updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s::vector, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT(id) DO UPDATE SET
                user_id = excluded.user_id,
                session_key = excluded.session_key,
                memory_type = excluded.memory_type,
                summary = excluded.summary,
                content_hash = excluded.content_hash,
                embedding = excluded.embedding,
                reinforcement = excluded.reinforcement,
                emotional_weight = excluded.emotional_weight,
                extra_json = excluded.extra_json,
                source_ref = excluded.source_ref,
                happened_at = excluded.happened_at,
                status = excluded.status,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            """,
            (
                row["id"],
                user_id,
                session_key,
                row["memory_type"],
                row["summary"],
                row["content_hash"],
                vector_literal(embedding),
                int(row["reinforcement"] or 1),
                int(row["emotional_weight"] or 0),
                Jsonb(extra_json),
                row["source_ref"],
                row["happened_at"],
                row["status"],
                row["created_at"],
                row["updated_at"],
            ),
        )
        count += 1
    return count


def migrate_consolidation_events(
    pg: psycopg.Connection[Any],
    *,
    db_path: Path,
    user_id: str,
    fallback_session_key: str | None,
) -> int:
    rows = sqlite_rows(db_path, "consolidation_events", copy_first=True)
    count = 0
    for row in rows:
        session_key = fallback_session_key
        if session_key:
            ensure_session(
                pg,
                session_key=session_key,
                user_id=user_id,
                created_at=str(row["created_at"]),
                updated_at=str(row["created_at"]),
            )
        pg.execute(
            """
            INSERT INTO consolidation_events(
                user_id, source_ref, item_id, session_key, created_at
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT(user_id, source_ref) DO UPDATE SET
                item_id = excluded.item_id,
                session_key = excluded.session_key,
                created_at = excluded.created_at
            """,
            (
                user_id,
                row["source_ref"],
                row["item_id"],
                session_key,
                row["created_at"],
            ),
        )
        count += 1
    return count


def migrate_memory_replacements(
    pg: psycopg.Connection[Any],
    *,
    db_path: Path,
    user_id: str,
    fallback_session_key: str | None,
) -> int:
    rows = sqlite_rows(db_path, "memory_replacements", copy_first=True)
    count = 0
    for row in rows:
        session_key = fallback_session_key
        if session_key:
            ensure_session(
                pg,
                session_key=session_key,
                user_id=user_id,
                created_at=str(row["created_at"]),
                updated_at=str(row["created_at"]),
            )
        pg.execute(
            """
            INSERT INTO memory_replacements(
                user_id, session_key, old_item_id, old_memory_type, old_summary,
                old_source_ref, old_happened_at, old_extra_json, new_item_id,
                new_memory_type, new_summary, new_source_ref, new_happened_at,
                new_extra_json, relation_type, source_ref, created_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                user_id,
                session_key,
                row["old_item_id"],
                row["old_memory_type"],
                row["old_summary"],
                row["old_source_ref"],
                row["old_happened_at"],
                Jsonb(parse_json(row["old_extra_json"], {})),
                row["new_item_id"],
                row["new_memory_type"],
                row["new_summary"],
                row["new_source_ref"],
                row["new_happened_at"],
                Jsonb(parse_json(row["new_extra_json"], {})),
                row["relation_type"] if "relation_type" in row.keys() else "supersede",
                row["source_ref"],
                row["created_at"],
            ),
        )
        count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate memory2 SQLite state to PostgreSQL.")
    parser.add_argument("--workspace", type=Path, default=Path.home() / ".akashic" / "workspace")
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    parser.add_argument(
        "--schema",
        type=Path,
        default=Path(__file__).with_name("memory2_pg_schema.sql"),
    )
    parser.add_argument("--user-id", default="")
    parser.add_argument("--user-email", default=DEFAULT_USER_EMAIL)
    parser.add_argument("--display-name", default="Telegram 8759439816")
    parser.add_argument("--session-key", default=DEFAULT_SESSION_KEY)
    parser.add_argument("--skip-schema", action="store_true")
    parser.add_argument(
        "--no-fallback-session",
        action="store_true",
        help="Do not write fallback session_key when an item cannot infer scope provenance.",
    )
    args = parser.parse_args()

    user_id = args.user_id.strip() or deterministic_uuid(f"user:{args.user_email.lower()}")
    db_path = args.db_path or (args.workspace / "memory" / "memory2.db")
    fallback_session_key = None if args.no_fallback_session else args.session_key

    with psycopg.connect(args.database_url) as pg:
        if not args.skip_schema:
            ensure_schema(pg, args.schema)
        ensure_user(
            pg,
            user_id=user_id,
            email=args.user_email,
            display_name=args.display_name,
        )
        memory_items = migrate_memory_items(
            pg,
            db_path=db_path,
            user_id=user_id,
            fallback_session_key=fallback_session_key,
        )
        consolidation_events = migrate_consolidation_events(
            pg,
            db_path=db_path,
            user_id=user_id,
            fallback_session_key=fallback_session_key,
        )
        memory_replacements = migrate_memory_replacements(
            pg,
            db_path=db_path,
            user_id=user_id,
            fallback_session_key=fallback_session_key,
        )
        pg.commit()

    print(f"user_id: {user_id}")
    print(f"db_path: {db_path}")
    print(f"memory_items: {memory_items}")
    print(f"consolidation_events: {consolidation_events}")
    print(f"memory_replacements: {memory_replacements}")


if __name__ == "__main__":
    main()
