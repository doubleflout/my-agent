from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]

from agent.lifecycle.types import BeforeTurnCtx, TurnState
from agent.plugins import Plugin
from agent.prompting import is_context_frame

logger = logging.getLogger("plugin.status_commands")

_SESSION_SLOT = "session:session"
_CTX_SLOT = "session:ctx"
_TS_PATTERN = re.compile(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})")
_BEIJING_TZ = ZoneInfo("Asia/Shanghai")


class MemoryStatusCommandModule:
    slot = "status_commands.memory_status"
    requires = ("before_turn.acquire_session", _SESSION_SLOT)
    produces = (_CTX_SLOT,)

    def __init__(self, plugin_name: str) -> None:
        self._plugin_name = plugin_name

    async def run(self, frame) -> object:
        if _CTX_SLOT in frame.slots:
            return frame
        state = frame.input
        command = _normalize_command(state.msg.content)
        if command not in {"/memorystatus", "/memory_status", "/compact_status"}:
            return frame
        session = state.session
        if session is None:
            return frame
        messages = list(getattr(session, "messages", []))
        last = max(0, int(getattr(session, "last_consolidated", 0)))
        last = min(last, len(messages))
        logger.info(
            "[%s:%s] hit command: %s",
            self._plugin_name,
            self.__class__.__name__,
            command,
        )
        frame.slots[_CTX_SLOT] = _abort_ctx(
            state, _format_memory_status_reply(messages, last)
        )
        return frame


class KVCacheCommandModule:
    slot = "status_commands.kvcache"
    requires = ("before_turn.acquire_session", _SESSION_SLOT)
    produces = (_CTX_SLOT,)

    def __init__(
        self,
        plugin_name: str,
        db_path: Path | None,
        *,
        database_url: str | None = None,
    ) -> None:
        self._plugin_name = plugin_name
        self._db_path = db_path
        self._database_url = (database_url or "").strip() or None

    async def run(self, frame) -> object:
        if _CTX_SLOT in frame.slots:
            return frame
        state = frame.input
        command = _normalize_command(state.msg.content)
        if command not in {"/kvcache", "/cache_status"}:
            return frame
        logger.info(
            "[%s:%s] hit command: %s",
            self._plugin_name,
            self.__class__.__name__,
            command,
        )
        reply = self._build_reply(state)
        frame.slots[_CTX_SLOT] = _abort_ctx(state, reply)
        return frame

    def _build_reply(self, state: TurnState) -> str:
        db_path = self._db_path
        if not self._database_url and (db_path is None or not db_path.exists()):
            return "No KVCache data yet (observe database not found)."

        args = (state.msg.content or "").strip().split()
        limit = 5
        if len(args) > 1:
            try:
                limit = max(1, min(30, int(args[1])))
            except ValueError:
                pass

        try:
            rows = self._load_rows(state.session_key, limit, db_path)
        except Exception:
            logger.exception("KVCache query failed")
            return "KVCache query failed."

        if not rows:
            return "No KVCache data yet."

        overall_prompt = sum(int(_row_values(r)[2] or 0) for r in rows)
        overall_hit = sum(int(_row_values(r)[3] or 0) for r in rows)
        overall_pct = (overall_hit / overall_prompt * 100) if overall_prompt > 0 else 0.0

        lines = [
            f"KVCache recent {len(rows)} turns",
            "",
            f"Hit rate {overall_pct:.1f}%  {_pct_bar(overall_pct)}",
            f"Token  {overall_hit:,} / {overall_prompt:,}",
        ]
        for row in rows:
            llm_output, ts, prompt_tokens, hit_tokens = _row_values(row)
            content = _content_to_text(llm_output or "")
            if is_context_frame(content):
                content = ""
            preview = _preview_text(content, limit=72)
            hit = int(hit_tokens or 0)
            prompt = int(prompt_tokens or 0)
            pct = (hit / prompt * 100) if prompt > 0 else 0.0
            lines.extend(["", ""])
            lines.append(
                f"{_format_ts(str(ts))}   {_pct_emoji(pct)} {pct:.1f}%  {_pct_bar(pct)}"
            )
            lines.append(f"    {hit:,} / {prompt:,} tokens")
            if preview:
                lines.append(f"    {preview}")
        return "\n".join(lines)

    def _load_rows(
        self,
        session_key: str,
        limit: int,
        db_path: Path | None,
    ) -> list[object]:
        if self._database_url:
            if psycopg is None:
                raise RuntimeError("psycopg is required for postgres status_commands")
            with psycopg.connect(self._database_url) as conn:
                return list(
                    conn.execute(
                        """
                        SELECT llm_output, ts, react_cache_prompt_tokens, react_cache_hit_tokens
                        FROM turns
                        WHERE session_key=%s AND source='agent'
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        [session_key, limit],
                    ).fetchall()
                )
        if db_path is None:
            return []
        conn = sqlite3.connect(str(db_path))
        try:
            return list(
                conn.execute(
                    """
                    SELECT llm_output, ts, react_cache_prompt_tokens, react_cache_hit_tokens
                    FROM turns
                    WHERE session_key=? AND source='agent'
                    ORDER BY id DESC
                    LIMIT ?
                    """,
                    [session_key, limit],
                ).fetchall()
            )
        finally:
            conn.close()


class StatusCommands(Plugin):
    name = "status_commands"

    def telegram_bot_commands(self) -> list[tuple[str, str]]:
        return [
            ("memorystatus", "Show memory consolidation status"),
            ("kvcache", "Show KVCache status"),
        ]

    def before_turn_modules(self) -> list[object]:
        plugin_name = self.name or "status_commands"
        db_path = None
        database_url = None
        if self.context.workspace is not None:
            db_path = self.context.workspace / "observe" / "observe.db"
        app_config = getattr(self.context, "app_config", None)
        storage = getattr(app_config, "storage", None)
        backend = str(getattr(storage, "backend", "") or "").strip().lower()
        if backend == "postgres":
            postgres = getattr(storage, "postgres", None)
            database_url = str(getattr(postgres, "database_url", "") or "").strip() or None
        return cast(
            "list[object]",
            [
                MemoryStatusCommandModule(plugin_name),
                KVCacheCommandModule(plugin_name, db_path, database_url=database_url),
            ],
        )


def _normalize_command(content: str) -> str:
    parts = (content or "").strip().split(maxsplit=1)
    if not parts:
        return ""
    head = parts[0].lower()
    if "@" in head:
        head = head.split("@", 1)[0]
    return head


def _abort_ctx(state: TurnState, reply: str) -> BeforeTurnCtx:
    return BeforeTurnCtx(
        session_key=state.session_key,
        channel=state.msg.channel,
        chat_id=state.msg.chat_id,
        content=state.msg.content,
        timestamp=state.msg.timestamp,
        skill_names=[],
        retrieved_memory_block="",
        retrieval_trace_raw=None,
        history_messages=(),
        abort=True,
        abort_reply=reply,
    )


def _format_ts(ts: str) -> str:
    try:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(_BEIJING_TZ)
        return f"{parsed.month}-{parsed.day} {parsed.hour:02d}:{parsed.minute:02d}"
    except ValueError:
        pass
    match = _TS_PATTERN.search(ts)
    if match:
        return f"{int(match.group(2))}-{int(match.group(3))} {match.group(4)}:{match.group(5)}"
    return ts


def _format_memory_status_reply(messages: list[dict], last_consolidated: int) -> str:
    consolidated_user = _count_real_user_messages(messages[:last_consolidated])
    total_user = _count_real_user_messages(messages)
    pending_user = max(0, total_user - consolidated_user)
    last_user_message = _latest_real_user_content(messages[:last_consolidated])

    lines = ["Memory consolidation status:"]
    if last_consolidated <= 0 or not last_user_message:
        lines.append("This session has not completed a memory consolidation yet.")
    elif pending_user == 0:
        lines.append("This session is already consolidated up to the latest user message.")
    else:
        lines.append(f"Last consolidation was before the most recent {pending_user} user messages.")
    if last_user_message:
        lines.extend(["", "Last consolidated user message:", f"\"{_preview_text(last_user_message)}\""])
    lines.extend(
        [
            "",
            f"Pending user messages: {pending_user}",
            f"Current session messages: {len(messages)}",
        ]
    )
    return "\n".join(lines)


def _count_real_user_messages(messages: list[dict]) -> int:
    return sum(1 for item in messages if _is_real_user_message(item))


def _latest_real_user_content(messages: list[dict]) -> str:
    for item in reversed(messages):
        if _is_real_user_message(item):
            return _content_to_text(item.get("content", ""))
    return ""


def _is_real_user_message(item: dict) -> bool:
    if item.get("role") != "user":
        return False
    content = _content_to_text(item.get("content", ""))
    return bool(content) and not is_context_frame(content)


def _content_to_text(content: object) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                parts.append(str(item.get("text", "")).strip())
        return "\n".join(part for part in parts if part).strip()
    return str(content).strip()


def _preview_text(text: str, limit: int = 80) -> str:
    normalized = " ".join(text.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def _pct_bar(pct: float, width: int = 10) -> str:
    filled = round(pct / 100 * width)
    filled = max(0, min(width, filled))
    return "#" * filled + "-" * (width - filled)


def _pct_emoji(pct: float) -> str:
    if pct >= 80:
        return "HIGH"
    if pct >= 40:
        return "MID"
    return "LOW"


def _row_values(row: object) -> tuple[Any, Any, Any, Any]:
    if hasattr(row, "__getitem__"):
        return row[0], row[1], row[2], row[3]
    raise TypeError(f"unsupported kvcache row type: {type(row)!r}")
