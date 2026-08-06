from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    select,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError


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
    status: str
    error: str | None
    created_at: datetime
    completed_at: datetime | None


class DuplicateEmailError(ValueError):
    pass


metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("email", String(320), nullable=False, unique=True, index=True),
    Column("password_hash", String(512), nullable=False),
    Column("display_name", String(120)),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("disabled", Boolean, nullable=False, default=False),
)

conversations = Table(
    "conversations",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("user_id", String(36), ForeignKey("users.id"), nullable=False, index=True),
    Column("title", String(200), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("archived", Boolean, nullable=False, default=False),
)

chat_messages = Table(
    "chat_messages",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("conversation_id", String(36), ForeignKey("conversations.id"), nullable=False, index=True),
    Column("user_id", String(36), ForeignKey("users.id"), nullable=False, index=True),
    Column("role", String(32), nullable=False),
    Column("content", Text, nullable=False),
    Column("metadata_json", Text, nullable=False, default="{}"),
    Column("created_at", DateTime(timezone=True), nullable=False),
)

agent_turns = Table(
    "agent_turns",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("conversation_id", String(36), ForeignKey("conversations.id"), nullable=False, index=True),
    Column("user_id", String(36), ForeignKey("users.id"), nullable=False, index=True),
    Column("status", String(32), nullable=False),
    Column("error", Text),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("completed_at", DateTime(timezone=True)),
)


def default_database_url(workspace: Path) -> str:
    db_path = workspace / "webapp.db"
    return "sqlite:///" + db_path.as_posix()


class WebStore:
    def __init__(self, database_url: str) -> None:
        connect_args = {}
        if database_url.startswith("sqlite"):
            connect_args["check_same_thread"] = False
        self.engine: Engine = create_engine(database_url, future=True, connect_args=connect_args)
        metadata.create_all(self.engine)

    def close(self) -> None:
        self.engine.dispose()

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

    def create_conversation(self, *, user_id: str, title: str | None) -> ConversationRecord:
        cid = str(uuid.uuid4())
        now = utcnow()
        clean_title = (title or "New chat").strip()[:200] or "New chat"
        with self._begin() as conn:
            conn.execute(
                conversations.insert().values(
                    id=cid,
                    user_id=user_id,
                    title=clean_title,
                    created_at=now,
                    updated_at=now,
                    archived=False,
                )
            )
        return ConversationRecord(cid, user_id, clean_title, now, now, False)

    def list_conversations(self, *, user_id: str) -> list[ConversationRecord]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(conversations)
                .where(conversations.c.user_id == user_id, conversations.c.archived == False)  # noqa: E712
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
        mid = str(uuid.uuid4())
        now = utcnow()
        meta = metadata or {}
        with self._begin() as conn:
            conn.execute(
                chat_messages.insert().values(
                    id=mid,
                    conversation_id=conversation_id,
                    user_id=user_id,
                    role=role,
                    content=content,
                    metadata_json=json.dumps(meta, ensure_ascii=False),
                    created_at=now,
                )
            )
            conn.execute(
                conversations.update()
                .where(conversations.c.id == conversation_id, conversations.c.user_id == user_id)
                .values(updated_at=now)
            )
        return MessageRecord(mid, conversation_id, user_id, role, content, meta, now)

    def list_messages(self, *, user_id: str, conversation_id: str) -> list[MessageRecord]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(chat_messages)
                .where(
                    chat_messages.c.user_id == user_id,
                    chat_messages.c.conversation_id == conversation_id,
                )
                .order_by(chat_messages.c.created_at.asc())
            ).mappings().all()
        return [self._message_from_row(row) for row in rows]

    def create_turn(self, *, user_id: str, conversation_id: str) -> TurnRecord:
        tid = str(uuid.uuid4())
        now = utcnow()
        with self._begin() as conn:
            conn.execute(
                agent_turns.insert().values(
                    id=tid,
                    conversation_id=conversation_id,
                    user_id=user_id,
                    status="pending",
                    error=None,
                    created_at=now,
                    completed_at=None,
                )
            )
        return TurnRecord(tid, conversation_id, user_id, "pending", None, now, None)

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
            title=str(row["title"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            archived=bool(row["archived"]),
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
    def _turn_from_row(row: Any) -> TurnRecord:
        return TurnRecord(
            id=str(row["id"]),
            conversation_id=str(row["conversation_id"]),
            user_id=str(row["user_id"]),
            status=str(row["status"]),
            error=row["error"],
            created_at=row["created_at"],
            completed_at=row["completed_at"],
        )

