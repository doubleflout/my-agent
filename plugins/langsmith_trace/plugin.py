from __future__ import annotations

import logging
import os
import threading
from typing import Any

from agent.plugins import Plugin
from bus.events_lifecycle import PhaseCompleted, TurnCommitted, TurnStarted

logger = logging.getLogger("plugin.langsmith_trace")


class LangSmithTracePlugin(Plugin):
    name = "langsmith_trace"

    async def initialize(self) -> None:
        config = _resolve_langsmith_config(self.context.app_config)
        if not _is_enabled(config):
            logger.info("langsmith trace disabled")
            return

        _configure_langsmith_env(config)
        try:
            client_cls, run_tree_cls = _load_langsmith()
        except RuntimeError as exc:
            logger.warning("%s", exc)
            return

        client = client_cls(**_client_kwargs(config))
        self._recorder = _TurnTraceRecorder(
            run_tree_cls=run_tree_cls,
            client=client,
            project=str(getattr(config, "project", "") or "akashic-agent"),
        )
        self.context.event_bus.on(TurnStarted, self._observe_turn_started)
        self.context.event_bus.on(PhaseCompleted, self._observe_phase_completed)
        self.context.event_bus.on(TurnCommitted, self._observe_turn_committed)
        logger.info("langsmith trace plugin loaded project=%s", self._recorder.project)

    async def terminate(self) -> None:
        recorder = getattr(self, "_recorder", None)
        if recorder is not None:
            recorder.close()

    def _observe_turn_started(self, event: TurnStarted) -> None:
        recorder = getattr(self, "_recorder", None)
        if recorder is None:
            return
        try:
            recorder.start(event)
        except Exception:
            logger.exception(
                "langsmith turn trace start failed session=%s turn=%s",
                event.session_key,
                event.turn_id,
            )

    def _observe_phase_completed(self, event: PhaseCompleted) -> None:
        recorder = getattr(self, "_recorder", None)
        if recorder is None:
            return
        try:
            recorder.record_phase(event)
        except Exception:
            logger.exception(
                "langsmith phase trace failed phase=%s session=%s turn=%s",
                event.phase,
                event.session_key,
                event.turn_id,
            )

    def _observe_turn_committed(self, event: TurnCommitted) -> None:
        recorder = getattr(self, "_recorder", None)
        if recorder is None:
            return
        try:
            recorder.finish(event)
        except Exception:
            logger.exception(
                "langsmith turn trace finish failed session=%s turn=%s",
                event.session_key,
                _committed_turn_id(event),
            )


class _TurnTraceRecorder:
    def __init__(
        self,
        *,
        run_tree_cls: type[Any],
        client: Any,
        project: str,
    ) -> None:
        self._run_tree_cls = run_tree_cls
        self._client = client
        self._active_runs: dict[str, Any] = {}
        self._pending_commits: dict[str, TurnCommitted] = {}
        self._after_turn_completed: set[str] = set()
        self._lock = threading.RLock()
        self.project = project

    def start(self, event: TurnStarted) -> None:
        turn_id = str(event.turn_id or "").strip()
        if not turn_id:
            logger.warning("langsmith turn start skipped: missing turn_id session=%s", event.session_key)
            return

        root = self._run_tree_cls(
            name="agent_turn",
            run_type="chain",
            inputs=_turn_started_inputs(event),
            extra={"metadata": _turn_started_metadata(event)},
            project_name=self.project,
            ls_client=self._client,
        )
        root.post()
        with self._lock:
            previous = self._active_runs.pop(turn_id, None)
            if previous is not None:
                previous.end(error="duplicate TurnStarted received for turn_id")
                previous.patch()
            self._pending_commits.pop(turn_id, None)
            self._after_turn_completed.discard(turn_id)
            self._active_runs[turn_id] = root

    def record_phase(self, event: PhaseCompleted) -> None:
        turn_id = str(event.turn_id or "").strip()
        if not turn_id:
            logger.warning(
                "langsmith phase skipped: missing turn_id phase=%s session=%s",
                event.phase,
                event.session_key,
            )
            return
        with self._lock:
            root = self._active_runs.get(turn_id)
            if root is None:
                logger.warning(
                    "langsmith phase skipped: root run not found phase=%s turn=%s",
                    event.phase,
                    turn_id,
                )
                return
            child = root.create_child(
                name=f"phase.{event.phase}",
                run_type="chain",
                inputs=dict(event.input_summary),
                extra={"metadata": _phase_metadata(event)},
            )
            child.post()
            child.end(outputs=dict(event.output_summary))
            child.patch()
            if event.phase == "after_turn":
                self._after_turn_completed.add(turn_id)
                committed = self._pending_commits.pop(turn_id, None)
                if committed is not None:
                    self._finish_root_locked(turn_id, root, committed)

    def finish(self, event: TurnCommitted) -> None:
        turn_id = _committed_turn_id(event)
        if not turn_id:
            logger.warning("langsmith turn finish skipped: missing turn_id session=%s", event.session_key)
            return
        with self._lock:
            root = self._active_runs.get(turn_id)
            if root is None:
                logger.warning("langsmith turn finish skipped: root run not found turn=%s", turn_id)
                return
            if turn_id in self._after_turn_completed:
                self._finish_root_locked(turn_id, root, event)
            else:
                self._pending_commits[turn_id] = event

    def _finish_root_locked(
        self,
        turn_id: str,
        root: Any,
        event: TurnCommitted,
    ) -> None:
        self._active_runs.pop(turn_id, None)
        self._pending_commits.pop(turn_id, None)
        self._after_turn_completed.discard(turn_id)
        error = event.extra.get("error")
        root.end(
            outputs=_turn_outputs(event),
            error=str(error) if error else None,
            metadata=_turn_metadata(event),
        )
        root.patch()

    def close(self) -> None:
        with self._lock:
            active = list(self._active_runs.values())
            self._active_runs.clear()
            self._pending_commits.clear()
            self._after_turn_completed.clear()
        for root in active:
            try:
                root.end(error="trace interrupted during plugin shutdown")
                root.patch()
            except Exception:
                logger.exception("langsmith unfinished trace close failed")
        try:
            self._client.flush()
        finally:
            self._client.close()


def _load_langsmith() -> tuple[type[Any], type[Any]]:
    try:
        from langsmith import Client
        from langsmith.run_trees import RunTree
    except ImportError as exc:
        raise RuntimeError(
            "LangSmith trace requested, but the 'langsmith' package is not installed. "
            "Install it with: pip install langsmith"
        ) from exc
    return Client, RunTree


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


def _client_kwargs(config: object) -> dict[str, object]:
    kwargs: dict[str, object] = {}
    values = {
        "api_key": getattr(config, "api_key", ""),
        "api_url": getattr(config, "endpoint", ""),
        "workspace_id": getattr(config, "workspace_id", ""),
    }
    for key, value in values.items():
        text = str(value or "").strip()
        if text:
            kwargs[key] = text
    return kwargs


def _set_env_if_present(key: str, value: object) -> None:
    text = str(value or "").strip()
    if text:
        os.environ[key] = text


def _committed_turn_id(event: TurnCommitted) -> str:
    return str(event.turn_id or event.extra.get("turn_id") or "").strip()


def _turn_started_metadata(event: TurnStarted) -> dict[str, object]:
    return {
        "turn_id": event.turn_id,
        "session_key": event.session_key,
        "channel": event.channel,
        "chat_id": event.chat_id,
    }


def _turn_started_inputs(event: TurnStarted) -> dict[str, object]:
    return {
        "session_key": event.session_key,
        "channel": event.channel,
        "chat_id": event.chat_id,
        "message": event.content,
        "timestamp": event.timestamp.isoformat(),
    }


def _turn_metadata(event: TurnCommitted) -> dict[str, object]:
    metadata: dict[str, object] = {
        "session_key": event.session_key,
        "channel": event.channel,
        "chat_id": event.chat_id,
    }
    turn_id = _committed_turn_id(event)
    if turn_id:
        metadata["turn_id"] = turn_id
    return metadata


def _phase_metadata(event: PhaseCompleted) -> dict[str, object]:
    metadata: dict[str, object] = dict(event.metadata)
    metadata.update(
        {
            "phase": event.phase,
            "session_key": event.session_key,
            "channel": event.channel,
            "chat_id": event.chat_id,
        }
    )
    if event.turn_id:
        metadata["turn_id"] = event.turn_id
    return metadata


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
