"""Optional LangSmith tracing adapter for LongMemEval.

This module keeps LangSmith out of the production agent path. It only wraps
benchmark QA calls when the eval config explicitly enables it.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from agent.config_models import LangSmithEvalConfig

from .dataset import LMEInstance
from .qa_runner import run_qa_instance
from .runtime import BenchmarkRuntime


def _load_langsmith() -> tuple[Any, Any]:
    try:
        from langsmith import traceable, tracing_context
    except ImportError as exc:
        raise RuntimeError(
            "LangSmith tracing requested, but the 'langsmith' package is not "
            "installed. Install it with: pip install langsmith"
        ) from exc
    return traceable, tracing_context


def configure_langsmith_env(config: LangSmithEvalConfig) -> None:
    """Apply optional config.toml LangSmith settings for the current process."""
    os.environ.setdefault("LANGSMITH_TRACING", "true")
    if config.api_key:
        os.environ["LANGSMITH_API_KEY"] = config.api_key
    if config.project:
        os.environ["LANGSMITH_PROJECT"] = config.project
    if config.endpoint:
        os.environ["LANGSMITH_ENDPOINT"] = config.endpoint
    if config.workspace_id:
        os.environ["LANGSMITH_WORKSPACE_ID"] = config.workspace_id


def _trace_inputs(instance: LMEInstance, workspace: Path) -> dict[str, Any]:
    return {
        "question_id": instance.question_id,
        "question_type": instance.question_type,
        "question": instance.question,
        "question_date": instance.question_date,
        "gold_answer": instance.answer,
        "session_key": instance.session_key,
        "qa_session_key": instance.qa_session_key,
        "workspace": str(workspace),
    }


def _trace_outputs(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "question_id": result.get("question_id"),
        "question_type": result.get("question_type"),
        "question": result.get("question"),
        "predicted_answer": result.get("predicted_answer"),
        "gold_answer": result.get("gold_answer"),
        "elapsed_s": result.get("elapsed_s"),
        "error": result.get("error"),
        "tool_chain": result.get("tool_chain") or [],
    }


async def run_langsmith_traced_qa(
    rt: BenchmarkRuntime,
    instance: LMEInstance,
    *,
    timeout_s: float,
    langsmith_config: LangSmithEvalConfig,
) -> dict[str, Any]:
    """Run one QA instance and publish a root LangSmith trace."""
    traceable, tracing_context = _load_langsmith()
    configure_langsmith_env(langsmith_config)

    @traceable(name="longmemeval_qa", run_type="chain")
    async def _target(inputs: dict[str, Any]) -> dict[str, Any]:
        result = await run_qa_instance(rt, instance, timeout_s=timeout_s)
        return _trace_outputs(result)

    with tracing_context(
        enabled=True,
        project_name=langsmith_config.project or None,
    ):
        traced = await _target(_trace_inputs(instance, rt.workspace))

    result = dict(traced)
    result.setdefault("question_id", instance.question_id)
    result.setdefault("question_type", instance.question_type)
    result.setdefault("question", instance.question)
    result.setdefault("gold_answer", instance.answer)
    result.setdefault("predicted_answer", "")
    result.setdefault("tool_chain", [])
    result.setdefault("elapsed_s", 0.0)
    result.setdefault("error", None)
    return result
