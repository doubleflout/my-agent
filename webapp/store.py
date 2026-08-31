from __future__ import annotations

import json
import os
import re
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote_plus

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    create_engine,
    func,
    inspect,
    select,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from proactive_v2.user_tick import compute_user_tick_interval


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class UserRecord:
    id: str
    email: str
    password_hash: str
    display_name: str | None
    disabled: bool
    created_at: datetime


@dataclass(frozen=True)
class ConversationRecord:
    id: str
    user_id: str
    session_key: str
    title: str
    created_at: datetime
    updated_at: datetime
    archived: bool


@dataclass(frozen=True)
class MessageRecord:
    id: str
    conversation_id: str
    user_id: str
    role: str
    content: str
    metadata: dict[str, Any]
    created_at: datetime


@dataclass(frozen=True)
class TurnRecord:
    id: str
    conversation_id: str
    user_id: str
    session_key: str
    status: str
    error: str | None
    created_at: datetime
    completed_at: datetime | None


@dataclass(frozen=True)
class ProactiveSessionRecord:
    id: str
    user_id: str
    conversation_id: str
    session_key: str
    enabled: bool
    last_tick_at: datetime | None
    next_tick_at: datetime
    interval_seconds: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class SkillRecord:
    id: str
    name: str
    title: str | None
    description: str
    skill_type: str
    scope: str
    user_id: str | None
    source: str
    relative_path: str
    entry_file: str
    metadata: dict[str, Any]
    enabled: bool
    created_at: datetime
    updated_at: datetime


class DuplicateEmailError(ValueError):
    pass


metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", Uuid(as_uuid=False), primary_key=True),
    Column("email", String(320), nullable=False, unique=True, index=True),
    Column("password_hash", String(512), nullable=False),
    Column("display_name", String(120)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("disabled", Boolean, nullable=False, default=False),
)

conversations = Table(
    "conversations",
    metadata,
    Column("id", Uuid(as_uuid=False), primary_key=True),
    Column("user_id", Uuid(as_uuid=False), ForeignKey("users.id"), nullable=False, index=True),
    Column("session_key", Text, nullable=False, unique=True),
    Column("title", String(200), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("archived", Boolean, nullable=False, default=False),
)

sessions = Table(
    "sessions",
    metadata,
    Column("key", Text, primary_key=True),
    Column("user_id", Uuid(as_uuid=False), ForeignKey("users.id")),
    Column("channel", Text, nullable=False),
    Column("chat_id", Text, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("last_consolidated", Integer, nullable=False, default=0),
    Column("metadata", JSON, nullable=False, default=dict),
    Column("last_user_at", DateTime(timezone=True)),
    Column("last_proactive_at", DateTime(timezone=True)),
    Column("next_seq", Integer, nullable=False, default=0),
)

messages = Table(
    "messages",
    metadata,
    Column("id", Text, primary_key=True),
    Column("session_key", Text, ForeignKey("sessions.key"), nullable=False, index=True),
    Column("user_id", Uuid(as_uuid=False), ForeignKey("users.id")),
    Column("seq", Integer, nullable=False),
    Column("role", Text, nullable=False),
    Column("content", Text),
    Column("tool_chain", JSON),
    Column("extra", JSON, nullable=False, default=dict),
    Column("ts", DateTime(timezone=True), nullable=False),
    UniqueConstraint("session_key", "seq", name="ux_messages_session_seq"),
)

agent_turns = Table(
    "agent_turns",
    metadata,
    Column("id", Uuid(as_uuid=False), primary_key=True),
    Column("conversation_id", Uuid(as_uuid=False), ForeignKey("conversations.id"), nullable=False, index=True),
    Column("user_id", Uuid(as_uuid=False), ForeignKey("users.id"), nullable=False, index=True),
    Column("session_key", Text, ForeignKey("sessions.key"), nullable=False),
    Column("status", String(32), nullable=False),
    Column("error", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True)),
)

proactive_sessions = Table(
    "proactive_sessions",
    metadata,
    Column("id", Uuid(as_uuid=False), primary_key=True),
    Column("user_id", Uuid(as_uuid=False), nullable=False, index=True),
    Column("conversation_id", Uuid(as_uuid=False), nullable=False, unique=True),
    Column("session_key", Text, nullable=False, unique=True),
    Column("enabled", Boolean, nullable=False, default=True),
    Column("last_tick_at", DateTime(timezone=True)),
    Column("next_tick_at", DateTime(timezone=True), nullable=False),
    Column("interval_seconds", Integer, nullable=False, default=4800),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)

skills = Table(
    "skills",
    metadata,
    Column("id", Uuid(as_uuid=False), primary_key=True),
    Column("name", Text, nullable=False),
    Column("title", Text),
    Column("description", Text, nullable=False),
    Column("skill_type", Text, nullable=False, default="normal"),
    Column("scope", Text, nullable=False, default="global"),
    Column("user_id", Uuid(as_uuid=False), ForeignKey("users.id")),
    Column("source", Text, nullable=False, default="filesystem"),
    Column("relative_path", Text, nullable=False),
    Column("entry_file", Text, nullable=False, default="SKILL.md"),
    Column("metadata_json", JSON, nullable=False, default=dict),
    Column("enabled", Boolean, nullable=False, default=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("scope", "user_id", "skill_type", "name", name="ux_skills_scope_user_type_name"),
)


def _web_session_key(user_id: str, conversation_id: str) -> str:
    return f"web:{user_id}:{conversation_id}"


def _web_proactive_session_key(user_id: str, conversation_id: str) -> str:
    return f"web:proactive:{user_id}:{conversation_id}"


def default_database_url(workspace: Path) -> str:
    db_path = workspace / "webapp.db"
    return "sqlite:///" + db_path.as_posix()


def database_url_from_config(config: Any | None, workspace: Path) -> str:
    env_url = os.environ.get("AKASHIC_WEB_DATABASE_URL", "").strip()
    if env_url:
        return env_url
    storage = getattr(config, "storage", None)
    if str(getattr(storage, "backend", "")).lower() != "postgres":
        return default_database_url(workspace)
    pg = getattr(storage, "postgres", None)
    configured = str(getattr(pg, "database_url", "") or "").strip()
    if configured:
        return configured
    host = str(getattr(pg, "host", "localhost") or "localhost")
    port = int(getattr(pg, "port", 5432) or 5432)
    database = str(getattr(pg, "database", "akashic_agent") or "akashic_agent")
    user = quote_plus(str(getattr(pg, "user", "postgres") or "postgres"))
    password = quote_plus(str(getattr(pg, "password", "") or ""))
    auth = user if not password else f"{user}:{password}"
    return f"postgresql+psycopg://{auth}@{host}:{port}/{database}"


def _skill_uuid(*parts: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "akashic:skill:" + ":".join(parts)))


def _parse_skill_frontmatter(path: Path) -> dict[str, Any]:
    try:
        content = path.read_text(encoding="utf-8")
    except Exception:
        return {}
    if not content.startswith("---"):
        return {}
    match = re.match(r"^---\n(.*?)\n---", content, re.DOTALL)
    if match is None:
        return {}
    metadata: dict[str, Any] = {}
    for line in match.group(1).splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        metadata[key.strip()] = value.strip().strip('"').strip("'")
    return metadata


def _scan_skill_dirs(root: Path) -> list[tuple[Path, dict[str, Any]]]:
    if not root.exists():
        return []
    rows: list[tuple[Path, dict[str, Any]]] = []
    for skill_dir in sorted(root.iterdir()):
        if not skill_dir.is_dir():
            continue
        skill_file = skill_dir / "SKILL.md"
        if not skill_file.exists():
            continue
        rows.append((skill_dir, _parse_skill_frontmatter(skill_file)))
    return rows


class WebStore:
    def __init__(self, database_url: str) -> None:
        connect_args = {}
        if database_url.startswith("sqlite"):
            connect_args["check_same_thread"] = False
        self.engine: Engine = create_engine(database_url, future=True, connect_args=connect_args)
        metadata.create_all(self.engine)
        self._ensure_schema_compat()

    def close(self) -> None:
        self.engine.dispose()

    def _ensure_schema_compat(self) -> None:
        inspector = inspect(self.engine)
        table_names = set(inspector.get_table_names())
        dialect = self.engine.dialect.name
        with self._begin() as conn:
            if "conversations" in table_names:
                columns = {col["name"] for col in inspector.get_columns("conversations")}
                if "session_key" not in columns:
                    conn.execute(text("ALTER TABLE conversations ADD COLUMN session_key TEXT"))
                conn.execute(
                    text(
                        """
                        UPDATE conversations
                        SET session_key = 'web:' || user_id || ':' || id
                        WHERE session_key IS NULL OR session_key = ''
                        """
                    )
                )
                if dialect == "sqlite":
                    conn.execute(
                        text(
                            "CREATE UNIQUE INDEX IF NOT EXISTS ux_conversations_session_key "
                            "ON conversations(session_key)"
                        )
                    )
                elif dialect == "postgresql":
                    conn.execute(
                        text(
                            "CREATE UNIQUE INDEX IF NOT EXISTS ux_conversations_session_key "
                            "ON conversations(session_key)"
                        )
                    )
            if "agent_turns" in table_names:
                columns = {col["name"] for col in inspector.get_columns("agent_turns")}
                if "session_key" not in columns:
                    conn.execute(text("ALTER TABLE agent_turns ADD COLUMN session_key TEXT"))
                conn.execute(
                    text(
                        """
                        UPDATE agent_turns
                        SET session_key = (
                            SELECT conversations.session_key
                            FROM conversations
                            WHERE conversations.id = agent_turns.conversation_id
                        )
                        WHERE session_key IS NULL OR session_key = ''
                        """
                    )
                )

    @contextmanager
    def _begin(self) -> Iterator[Any]:
        with self.engine.begin() as conn:
            yield conn

    def create_user(self, *, email: str, password_hash: str, display_name: str | None) -> UserRecord:
        user_id = str(uuid.uuid4())
        now = utcnow()
        normalized = email.strip().lower()
        with self._begin() as conn:
            try:
                conn.execute(
                    users.insert().values(
                        id=user_id,
                        email=normalized,
                        password_hash=password_hash,
                        display_name=display_name,
                        created_at=now,
                        disabled=False,
                    )
                )
            except IntegrityError as exc:
                raise DuplicateEmailError(normalized) from exc
        return UserRecord(user_id, normalized, password_hash, display_name, False, now)

    def get_user_by_email(self, email: str) -> UserRecord | None:
        normalized = email.strip().lower()
        with self.engine.connect() as conn:
            row = conn.execute(select(users).where(users.c.email == normalized)).mappings().first()
        return self._user_from_row(row) if row else None

    def get_user(self, user_id: str) -> UserRecord | None:
        with self.engine.connect() as conn:
            row = conn.execute(select(users).where(users.c.id == user_id)).mappings().first()
        return self._user_from_row(row) if row else None

    def sync_skills_from_filesystem(
        self,
        *,
        user_id: str,
        user_workspace: Path,
        global_skills_dir: Path,
    ) -> list[SkillRecord]:
        now = utcnow()
        candidates: list[dict[str, Any]] = []

        def add_candidates(
            *,
            root: Path,
            scope: str,
            skill_type: str,
            owner_id: str | None,
            relative_prefix: str,
            skip_names: set[str] | None = None,
        ) -> None:
            for skill_dir, meta in _scan_skill_dirs(root):
                name = str(meta.get("name") or skill_dir.name).strip() or skill_dir.name
                if skip_names and name in skip_names:
                    continue
                description = str(meta.get("description") or name).strip()
                candidates.append(
                    {
                        "id": _skill_uuid(scope, owner_id or "global", skill_type, name),
                        "name": name,
                        "title": str(meta.get("title") or "") or None,
                        "description": description,
                        "skill_type": skill_type,
                        "scope": scope,
                        "user_id": owner_id,
                        "source": "filesystem",
                        "relative_path": f"{relative_prefix}/{skill_dir.name}".replace("\\", "/"),
                        "entry_file": "SKILL.md",
                        "metadata_json": meta,
                        "enabled": True,
                        "created_at": now,
                        "updated_at": now,
                    }
                )

        global_names = {
            str(meta.get("name") or skill_dir.name).strip() or skill_dir.name
            for skill_dir, meta in _scan_skill_dirs(global_skills_dir)
        }
        add_candidates(
            root=global_skills_dir,
            scope="global",
            skill_type="normal",
            owner_id=None,
            relative_prefix="skills",
        )
        add_candidates(
            root=user_workspace / "skills",
            scope="user",
            skill_type="normal",
            owner_id=user_id,
            relative_prefix="skills",
            skip_names=global_names,
        )
        add_candidates(
            root=user_workspace / "drift" / "skills",
            scope="user",
            skill_type="drift",
            owner_id=user_id,
            relative_prefix="drift/skills",
        )

        with self._begin() as conn:
            for row in candidates:
                existing = conn.execute(select(skills.c.id).where(skills.c.id == row["id"])).first()
                if existing:
                    conn.execute(
                        skills.update()
                        .where(skills.c.id == row["id"])
                        .values(
                            title=row["title"],
                            description=row["description"],
                            source=row["source"],
                            relative_path=row["relative_path"],
                            entry_file=row["entry_file"],
                            metadata_json=row["metadata_json"],
                            updated_at=now,
                        )
                    )
                else:
                    conn.execute(skills.insert().values(**row))
        return self.list_skills(user_id=user_id)

    def list_skills(self, *, user_id: str) -> list[SkillRecord]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(skills)
                .where(
                    (skills.c.scope == "global")
                    | ((skills.c.scope == "user") & (skills.c.user_id == user_id))
                )
                .order_by(skills.c.scope.asc(), skills.c.skill_type.asc(), skills.c.name.asc())
            ).mappings().all()
        return [self._skill_from_row(row) for row in rows]

    def set_user_skill_enabled(
        self,
        *,
        user_id: str,
        skill_id: str,
        enabled: bool,
    ) -> SkillRecord | None:
        now = utcnow()
        with self._begin() as conn:
            row = conn.execute(
                select(skills).where(
                    skills.c.id == skill_id,
                    skills.c.scope == "user",
                    skills.c.user_id == user_id,
                )
            ).mappings().first()
            if row is None:
                return None
            conn.execute(
                skills.update()
                .where(skills.c.id == skill_id)
                .values(enabled=bool(enabled), updated_at=now)
            )
            updated = conn.execute(select(skills).where(skills.c.id == skill_id)).mappings().one()
        return self._skill_from_row(updated)

    def upsert_skill_record(
        self,
        *,
        user_id: str,
        name: str,
        skill_type: str,
        scope: str = "user",
        title: str | None = None,
        description: str = "",
        source: str = "filesystem",
        relative_path: str,
        entry_file: str = "SKILL.md",
        metadata: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> SkillRecord:
        clean_name = name.strip()
        clean_scope = scope.strip() or "user"
        owner_id = user_id if clean_scope == "user" else None
        now = utcnow()
        row_id = _skill_uuid(clean_scope, owner_id or "global", skill_type, clean_name)
        values = {
            "id": row_id,
            "name": clean_name,
            "title": title,
            "description": description.strip() or clean_name,
            "skill_type": skill_type.strip() or "normal",
            "scope": clean_scope,
            "user_id": owner_id,
            "source": source.strip() or "filesystem",
            "relative_path": relative_path.strip().replace("\\", "/"),
            "entry_file": entry_file.strip() or "SKILL.md",
            "metadata_json": metadata or {},
            "enabled": bool(enabled),
            "created_at": now,
            "updated_at": now,
        }
        with self._begin() as conn:
            existing = conn.execute(select(skills.c.id).where(skills.c.id == row_id)).first()
            if existing:
                conn.execute(
                    skills.update()
                    .where(skills.c.id == row_id)
                    .values(
                        title=values["title"],
                        description=values["description"],
                        source=values["source"],
                        relative_path=values["relative_path"],
                        entry_file=values["entry_file"],
                        metadata_json=values["metadata_json"],
                        enabled=values["enabled"],
                        updated_at=now,
                    )
                )
            else:
                conn.execute(skills.insert().values(**values))
            row = conn.execute(select(skills).where(skills.c.id == row_id)).mappings().one()
        return self._skill_from_row(row)

    def delete_skill_record(
        self,
        *,
        user_id: str,
        name: str,
        skill_type: str = "normal",
        scope: str = "user",
    ) -> bool:
        clean_scope = scope.strip() or "user"
        owner_id = user_id if clean_scope == "user" else None
        row_id = _skill_uuid(clean_scope, owner_id or "global", skill_type, name.strip())
        with self._begin() as conn:
            result = conn.execute(skills.delete().where(skills.c.id == row_id))
        return bool(result.rowcount and result.rowcount > 0)

    def create_conversation(self, *, user_id: str, title: str | None) -> ConversationRecord:
        cid = str(uuid.uuid4())
        now = utcnow()
        clean_title = (title or "New chat").strip()[:200] or "New chat"
        session_key = _web_session_key(user_id, cid)
        with self._begin() as conn:
            self._ensure_session_row(
                conn,
                user_id=user_id,
                conversation_id=cid,
                session_key=session_key,
                channel="web",
                now=now,
            )
            conn.execute(
                conversations.insert().values(
                    id=cid,
                    user_id=user_id,
                    session_key=session_key,
                    title=clean_title,
                    created_at=now,
                    updated_at=now,
                    archived=False,
                )
            )
        return ConversationRecord(cid, user_id, session_key, clean_title, now, now, False)

    def ensure_default_proactive_session(
        self,
        *,
        user_id: str,
        title: str = "主动推送",
        interval_seconds: int = 4800,
    ) -> ConversationRecord:
        now = utcnow()
        clean_title = title.strip()[:200] or "Proactive"
        with self._begin() as conn:
            existing = conn.execute(
                select(conversations)
                .select_from(
                    conversations.join(
                        proactive_sessions,
                        conversations.c.id == proactive_sessions.c.conversation_id,
                    )
                )
                .where(
                    proactive_sessions.c.user_id == user_id,
                    proactive_sessions.c.enabled == True,  # noqa: E712
                    conversations.c.archived == False,  # noqa: E712
                )
                .order_by(proactive_sessions.c.created_at.asc())
                .limit(1)
            ).mappings().first()
            if existing is not None:
                return self._conversation_from_row(existing)

            cid = str(uuid.uuid4())
            proactive_id = str(uuid.uuid4())
            session_key = _web_proactive_session_key(user_id, cid)
            self._ensure_session_row(
                conn,
                user_id=user_id,
                conversation_id=cid,
                session_key=session_key,
                channel="web_proactive",
                now=now,
            )
            conn.execute(
                conversations.insert().values(
                    id=cid,
                    user_id=user_id,
                    session_key=session_key,
                    title=clean_title,
                    created_at=now,
                    updated_at=now,
                    archived=False,
                )
            )
            conn.execute(
                proactive_sessions.insert().values(
                    id=proactive_id,
                    user_id=user_id,
                    conversation_id=cid,
                    session_key=session_key,
                    enabled=True,
                    last_tick_at=None,
                    next_tick_at=now,
                    interval_seconds=max(1, int(interval_seconds)),
                    created_at=now,
                    updated_at=now,
                )
            )
        return ConversationRecord(cid, user_id, session_key, clean_title, now, now, False)

    def get_default_proactive_conversation(self, *, user_id: str) -> ConversationRecord | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                select(conversations)
                .select_from(
                    conversations.join(
                        proactive_sessions,
                        conversations.c.id == proactive_sessions.c.conversation_id,
                    )
                )
                .where(
                    proactive_sessions.c.user_id == user_id,
                    proactive_sessions.c.enabled == True,  # noqa: E712
                    conversations.c.archived == False,  # noqa: E712
                )
                .order_by(proactive_sessions.c.created_at.asc())
                .limit(1)
            ).mappings().first()
        return self._conversation_from_row(row) if row is not None else None

    def list_due_proactive_sessions(
        self,
        *,
        now: datetime | None = None,
        limit: int = 50,
    ) -> list[ProactiveSessionRecord]:
        tick_now = now or utcnow()
        safe_limit = max(1, min(int(limit), 500))
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(proactive_sessions)
                .where(
                    proactive_sessions.c.enabled == True,  # noqa: E712
                    proactive_sessions.c.next_tick_at <= tick_now,
                )
                .order_by(proactive_sessions.c.next_tick_at.asc())
                .limit(safe_limit)
            ).mappings().all()
        return [self._proactive_session_from_row(row) for row in rows]

    def user_last_user_times(self, *, user_id: str) -> list[datetime | None]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(sessions.c.last_user_at).where(sessions.c.user_id == user_id)
            ).all()
        return [row[0] for row in rows]

    def schedule_next_proactive_tick(
        self,
        *,
        session_key: str,
        cfg: Any,
        now: datetime | None = None,
    ) -> int:
        tick_now = now or utcnow()
        with self._begin() as conn:
            row = conn.execute(
                select(proactive_sessions.c.user_id).where(
                    proactive_sessions.c.session_key == session_key,
                    proactive_sessions.c.enabled == True,  # noqa: E712
                )
            ).mappings().first()
            if row is None:
                raise KeyError(session_key)
            user_id = str(row["user_id"])
            user_times = [
                item[0]
                for item in conn.execute(
                    select(sessions.c.last_user_at).where(sessions.c.user_id == user_id)
                ).all()
            ]
            interval = compute_user_tick_interval(
                cfg=cfg,
                user_last_user_at=user_times,
                now=tick_now,
            )
            conn.execute(
                proactive_sessions.update()
                .where(proactive_sessions.c.session_key == session_key)
                .values(
                    last_tick_at=tick_now,
                    next_tick_at=tick_now + timedelta(seconds=interval),
                    interval_seconds=interval,
                    updated_at=tick_now,
                )
            )
        return interval

    def list_conversations(self, *, user_id: str) -> list[ConversationRecord]:
        with self.engine.connect() as conn:
            proactive_conversation_ids = select(proactive_sessions.c.conversation_id).where(
                proactive_sessions.c.user_id == user_id,
                proactive_sessions.c.enabled == True,  # noqa: E712
            )
            rows = conn.execute(
                select(conversations)
                .where(
                    conversations.c.user_id == user_id,
                    conversations.c.archived == False,  # noqa: E712
                    conversations.c.id.not_in(proactive_conversation_ids),
                )
                .order_by(conversations.c.updated_at.desc())
            ).mappings().all()
        return [self._conversation_from_row(row) for row in rows]

    def get_conversation(self, *, user_id: str, conversation_id: str) -> ConversationRecord | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                select(conversations).where(
                    conversations.c.id == conversation_id,
                    conversations.c.user_id == user_id,
                    conversations.c.archived == False,  # noqa: E712
                )
            ).mappings().first()
        return self._conversation_from_row(row) if row else None

    def add_message(
        self,
        *,
        conversation_id: str,
        user_id: str,
        role: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> MessageRecord:
        now = utcnow()
        meta = metadata or {}
        with self._begin() as conn:
            conv = conn.execute(
                select(conversations.c.session_key).where(
                    conversations.c.id == conversation_id,
                    conversations.c.user_id == user_id,
                )
            ).mappings().first()
            if conv is None:
                raise KeyError(conversation_id)
            session_key = str(conv["session_key"])
            session_seq_row = conn.execute(
                select(sessions.c.next_seq).where(sessions.c.key == session_key)
            ).mappings().first()
            message_seq_row = conn.execute(
                select(func.max(messages.c.seq)).where(messages.c.session_key == session_key)
            ).first()
            session_next = int((session_seq_row["next_seq"] if session_seq_row else 0) or 0)
            message_next = int((message_seq_row[0] if message_seq_row and message_seq_row[0] is not None else -1) + 1)
            seq = max(session_next, message_next)
            mid = f"{session_key}:{seq}"
            conn.execute(
                messages.insert().values(
                    id=mid,
                    session_key=session_key,
                    user_id=user_id,
                    seq=seq,
                    role=role,
                    content=content,
                    tool_chain=None,
                    extra=meta,
                    ts=now,
                )
            )
            session_values: dict[str, Any] = {
                "updated_at": now,
                "next_seq": seq + 1,
            }
            if role == "user":
                session_values["last_user_at"] = now
            if meta.get("source") == "proactive":
                session_values["last_proactive_at"] = now
            conn.execute(
                sessions.update()
                .where(sessions.c.key == session_key)
                .values(**session_values)
            )
            conn.execute(
                conversations.update()
                .where(conversations.c.id == conversation_id, conversations.c.user_id == user_id)
                .values(updated_at=now)
            )
        return MessageRecord(mid, conversation_id, user_id, role, content, meta, now)

    def list_messages(self, *, user_id: str, conversation_id: str) -> list[MessageRecord]:
        with self.engine.connect() as conn:
            conv = conn.execute(
                select(conversations.c.session_key).where(
                    conversations.c.id == conversation_id,
                    conversations.c.user_id == user_id,
                    conversations.c.archived == False,  # noqa: E712
                )
            ).mappings().first()
            if conv is None:
                return []
            session_key = str(conv["session_key"])
            rows = conn.execute(
                select(messages)
                .where(
                    messages.c.session_key == session_key,
                )
                .order_by(messages.c.seq.asc(), messages.c.ts.asc())
            ).mappings().all()
        return [self._message_from_session_row(row, conversation_id=conversation_id, user_id=user_id) for row in rows]

    def create_turn(self, *, user_id: str, conversation_id: str) -> TurnRecord:
        tid = str(uuid.uuid4())
        now = utcnow()
        with self._begin() as conn:
            conv = conn.execute(
                select(conversations.c.session_key).where(
                    conversations.c.id == conversation_id,
                    conversations.c.user_id == user_id,
                )
            ).mappings().first()
            if conv is None:
                raise KeyError(conversation_id)
            session_key = str(conv["session_key"])
            conn.execute(
                agent_turns.insert().values(
                    id=tid,
                    conversation_id=conversation_id,
                    user_id=user_id,
                    session_key=session_key,
                    status="pending",
                    error=None,
                    created_at=now,
                    completed_at=None,
                )
            )
        return TurnRecord(tid, conversation_id, user_id, session_key, "pending", None, now, None)

    @staticmethod
    def _ensure_session_row(
        conn: Any,
        *,
        user_id: str,
        conversation_id: str,
        session_key: str,
        channel: str,
        now: datetime,
    ) -> None:
        existing = conn.execute(
            select(sessions.c.key).where(sessions.c.key == session_key).limit(1)
        ).first()
        if existing is not None:
            return
        conn.execute(
            sessions.insert().values(
                key=session_key,
                user_id=user_id,
                channel=channel,
                chat_id=conversation_id,
                created_at=now,
                updated_at=now,
                last_consolidated=0,
                metadata={},
                last_user_at=None,
                last_proactive_at=None,
                next_seq=0,
            )
        )

    def get_turn(self, *, user_id: str, turn_id: str) -> TurnRecord | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                select(agent_turns).where(agent_turns.c.id == turn_id, agent_turns.c.user_id == user_id)
            ).mappings().first()
        return self._turn_from_row(row) if row else None

    def update_turn(self, *, turn_id: str, status: str, error: str | None = None) -> None:
        values: dict[str, Any] = {"status": status}
        if error is not None:
            values["error"] = error
        if status in {"completed", "failed"}:
            values["completed_at"] = utcnow()
        with self._begin() as conn:
            conn.execute(agent_turns.update().where(agent_turns.c.id == turn_id).values(**values))

    @staticmethod
    def _user_from_row(row: Any) -> UserRecord:
        return UserRecord(
            id=str(row["id"]),
            email=str(row["email"]),
            password_hash=str(row["password_hash"]),
            display_name=row["display_name"],
            disabled=bool(row["disabled"]),
            created_at=row["created_at"],
        )

    @staticmethod
    def _conversation_from_row(row: Any) -> ConversationRecord:
        return ConversationRecord(
            id=str(row["id"]),
            user_id=str(row["user_id"]),
            session_key=str(row["session_key"]),
            title=str(row["title"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            archived=bool(row["archived"]),
        )

    @staticmethod
    def _proactive_session_from_row(row: Any) -> ProactiveSessionRecord:
        return ProactiveSessionRecord(
            id=str(row["id"]),
            user_id=str(row["user_id"]),
            conversation_id=str(row["conversation_id"]),
            session_key=str(row["session_key"]),
            enabled=bool(row["enabled"]),
            last_tick_at=row["last_tick_at"],
            next_tick_at=row["next_tick_at"],
            interval_seconds=int(row["interval_seconds"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _skill_from_row(row: Any) -> SkillRecord:
        raw_meta = row["metadata_json"] or {}
        if isinstance(raw_meta, str):
            try:
                meta = json.loads(raw_meta)
            except Exception:
                meta = {}
        elif isinstance(raw_meta, dict):
            meta = dict(raw_meta)
        else:
            meta = {}
        return SkillRecord(
            id=str(row["id"]),
            name=str(row["name"]),
            title=str(row["title"]) if row["title"] else None,
            description=str(row["description"] or ""),
            skill_type=str(row["skill_type"]),
            scope=str(row["scope"]),
            user_id=str(row["user_id"]) if row["user_id"] else None,
            source=str(row["source"]),
            relative_path=str(row["relative_path"]),
            entry_file=str(row["entry_file"]),
            metadata=meta,
            enabled=bool(row["enabled"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    @staticmethod
    def _message_from_row(row: Any) -> MessageRecord:
        try:
            meta = json.loads(row["metadata_json"] or "{}")
        except Exception:
            meta = {}
        return MessageRecord(
            id=str(row["id"]),
            conversation_id=str(row["conversation_id"]),
            user_id=str(row["user_id"]),
            role=str(row["role"]),
            content=str(row["content"]),
            metadata=meta,
            created_at=row["created_at"],
        )

    @staticmethod
    def _message_from_session_row(
        row: Any,
        *,
        conversation_id: str,
        user_id: str,
    ) -> MessageRecord:
        extra = row["extra"] or {}
        if isinstance(extra, str):
            try:
                meta = json.loads(extra or "{}")
            except Exception:
                meta = {}
        elif isinstance(extra, dict):
            meta = dict(extra)
        else:
            meta = {}
        return MessageRecord(
            id=str(row["id"]),
            conversation_id=conversation_id,
            user_id=str(row["user_id"] or user_id),
            role=str(row["role"]),
            content=str(row["content"] or ""),
            metadata=meta,
            created_at=row["ts"],
        )

    @staticmethod
    def _turn_from_row(row: Any) -> TurnRecord:
        return TurnRecord(
            id=str(row["id"]),
            conversation_id=str(row["conversation_id"]),
            user_id=str(row["user_id"]),
            session_key=str(row["session_key"]),
            status=str(row["status"]),
            error=row["error"],
            created_at=row["created_at"],
            completed_at=row["completed_at"],
        )
