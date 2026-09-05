from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any

from agent.plugins import Plugin
from bus.events_lifecycle import PhaseCompleted, TurnCommitted

logger = logging.getLogger("plugin.langsmith_trace")


class LangSmithTracePlugin(Plugin):
    name = "langsmith_trace"

    async def initialize(self) -> None:
        config = _resolve_langsmith_config(self.context.app_config)
        if not _is_enabled(config):
            logger.info("langsmith trace disabled")
            return

        try:
            traceable, tracing_context = _load_langsmith()
        except RuntimeError as exc:
            logger.warning("%s", exc)
            return

        _configure_langsmith_env(config)
        self._recorder = _TurnTraceRecorder(
            traceable=traceable,
            tracing_context=tracing_context,
            project=str(getattr(config, "project", "") or "akashic-agent"),
        )
        self.context.event_bus.on(TurnCommitted, self._observe_turn_committed)
        self.context.event_bus.on(PhaseCompleted, self._observe_phase_completed)
        logger.info("langsmith trace plugin loaded project=%s", self._recorder.project)

    def _observe_turn_committed(self, event: TurnCommitted) -> None:
        recorder = getattr(self, "_recorder", None)
        if recorder is None:
            return
        try:
            recorder.record(event)
        except Exception:
            logger.exception(
                "langsmith turn trace failed session=%s",
                event.session_key,
            )

    def _observe_phase_completed(self, event: PhaseCompleted) -> None:
        recorder = getattr(self, "_recorder", None)
        if recorder is None:
            return
        try:
            recorder.record_phase(event)
        except Exception:
            logger.exception(
                "langsmith phase trace failed phase=%s session=%s",
                event.phase,
                event.session_key,
            )


class _TurnTraceRecorder:
    def __init__(
        self,
        *,
        traceable: Callable[..., Callable[[Callable[[dict[str, object]], dict[str, object]]], Callable[[dict[str, object]], dict[str, object]]]],
        tracing_context: Callable[..., Any],
        project: str,
    ) -> None:
        self._traceable = traceable
        self._tracing_context = tracing_context
        self.project = project

    def record(self, event: TurnCommitted) -> dict[str, object]:
        metadata = _turn_metadata(event)

        @self._traceable(
            name="agent_turn",
            run_type="chain",
            metadata=metadata,
        )
        def _record_turn(payload: dict[str, object]) -> dict[str, object]:
            return _turn_outputs(event)

        with self._tracing_context(enabled=True, project_name=self.project):
            return _record_turn(_turn_inputs(event))

    def record_phase(self, event: PhaseCompleted) -> dict[str, object]:
        metadata = _phase_metadata(event)

        @self._traceable(
            name=f"phase.{event.phase}",
            run_type="chain",
            metadata=metadata,
        )
        def _record_phase(payload: dict[str, object]) -> dict[str, object]:
            return dict(event.output_summary)

        with self._tracing_context(enabled=True, project_name=self.project):
            return _record_phase(dict(event.input_summary))


def _load_langsmith() -> tuple[Callable[..., Any], Callable[..., Any]]:
    try:
        from langsmith import traceable, tracing_context
    except ImportError as exc:
        raise RuntimeError(
            "LangSmith trace requested, but the 'langsmith' package is not installed. "
            "Install it with: pip install langsmith"
        ) from exc
    return traceable, tracing_context


def _resolve_langsmith_config(app_config: object | None) -> object | None:
    eval_config = getattr(app_config, "eval", None)
    return getattr(eval_config, "langsmith", None)


def _is_enabled(config: object | None) -> bool:
    if config is None:
        return False
    return bool(getattr(config, "enabled", False))


def _configure_langsmith_env(config: object) -> None:
    os.environ["LANGSMITH_TRACING"] = "true"
    _set_env_if_present("LANGSMITH_API_KEY", getattr(config, "api_key", ""))
    _set_env_if_present("LANGSMITH_PROJECT", getattr(config, "project", ""))
    _set_env_if_present("LANGSMITH_ENDPOINT", getattr(config, "endpoint", ""))
    _set_env_if_present("LANGSMITH_WORKSPACE_ID", getattr(config, "workspace_id", ""))


def _set_env_if_present(key: str, value: object) -> None:
    text = str(value or "").strip()
    if text:
        os.environ[key] = text


def _turn_metadata(event: TurnCommitted) -> dict[str, object]:
    metadata: dict[str, object] = {
        "session_key": event.session_key,
        "channel": event.channel,
        "chat_id": event.chat_id,
    }
    turn_id = event.extra.get("turn_id")
    if turn_id:
        metadata["turn_id"] = str(turn_id)
    return metadata


def _phase_metadata(event: PhaseCompleted) -> dict[str, object]:
    metadata: dict[str, object] = {
        "phase": event.phase,
        "session_key": event.session_key,
        "channel": event.channel,
        "chat_id": event.chat_id,
    }
    metadata.update(dict(event.metadata))
    return metadata


def _turn_inputs(event: TurnCommitted) -> dict[str, object]:
    return {
        "session_key": event.session_key,
        "channel": event.channel,
        "chat_id": event.chat_id,
        "message": event.input_message,
        "persisted_user_message": event.persisted_user_message,
    }


def _turn_outputs(event: TurnCommitted) -> dict[str, object]:
    return {
        "assistant_response": event.assistant_response,
        "raw_reply": event.raw_reply,
        "tools_used": event.tools_used,
        "tool_chain": event.tool_chain_raw,
        "post_reply_budget": event.post_reply_budget,
        "react_stats": event.react_stats,
        "error": event.extra.get("error"),
    }
