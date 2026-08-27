"""Upload the local LongMemEval JSON file as a LangSmith dataset.

This script only creates/updates the dataset examples. It does not run the
agent, does not ingest memories, and does not create experiment runs.
"""

from __future__ import annotations

import argparse
from typing import Any

from agent.config import load_config

from .dataset import SUPPORTED_QUESTION_TYPES, LMEInstance, load_dataset
from .langsmith_adapter import configure_langsmith_env


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Upload eval/longmemeval JSON examples to LangSmith."
    )
    p.add_argument("--config", required=True)
    p.add_argument("--data", required=True)
    p.add_argument(
        "--dataset-name",
        default=None,
        help="Override eval.langsmith.dataset from config.toml",
    )
    p.add_argument(
        "--description",
        default="Akashic Agent LongMemEval subset for memory QA evaluation.",
    )
    p.add_argument("--limit", type=int, default=0)
    p.add_argument(
        "--type",
        dest="question_type",
        default=None,
        help="Filter to one supported question_type.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the converted example shape without uploading.",
    )
    return p


def _example_from_instance(instance: LMEInstance) -> dict[str, Any]:
    inputs = {
        "question_id": instance.question_id,
        "question_type": instance.question_type,
        "question": instance.question,
        "question_date": instance.question_date,
        "haystack_dates": instance.haystack_dates,
        "haystack_session_ids": instance.haystack_session_ids,
        "haystack_sessions": [
            [
                {
                    "role": turn.role,
                    "content": turn.content,
                    "has_answer": turn.has_answer,
                }
                for turn in session
            ]
            for session in instance.haystack_sessions
        ],
        "answer_session_ids": instance.answer_session_ids,
    }
    outputs = {
        "answer": instance.answer,
    }
    metadata = {
        "question_id": instance.question_id,
        "question_type": instance.question_type,
        "session_key": instance.session_key,
        "qa_session_key": instance.qa_session_key,
    }
    return {
        "inputs": inputs,
        "outputs": outputs,
        "metadata": metadata,
        "split": instance.question_type,
    }


def _load_langsmith_client():
    try:
        from langsmith import Client
    except ImportError as exc:
        raise RuntimeError(
            "LangSmith dataset upload requested, but the 'langsmith' package is "
            "not installed. Install it with: pip install langsmith"
        ) from exc
    return Client


def _filter_instances(
    instances: list[LMEInstance],
    *,
    question_type: str | None,
    limit: int,
) -> list[LMEInstance]:
    filtered = [i for i in instances if i.question_type in SUPPORTED_QUESTION_TYPES]
    if question_type:
        if question_type not in SUPPORTED_QUESTION_TYPES:
            choices = ", ".join(SUPPORTED_QUESTION_TYPES)
            raise SystemExit(f"unsupported --type {question_type!r}; choices: {choices}")
        filtered = [i for i in filtered if i.question_type == question_type]
    if limit > 0:
        filtered = filtered[:limit]
    return filtered


def main() -> None:
    args = _build_parser().parse_args()
    config = load_config(args.config)
    langsmith_config = config.eval.langsmith
    dataset_name = args.dataset_name or langsmith_config.dataset
    if not dataset_name:
        raise SystemExit("LangSmith dataset name is empty.")

    instances = _filter_instances(
        load_dataset(args.data),
        question_type=args.question_type,
        limit=args.limit,
    )
    examples = [_example_from_instance(instance) for instance in instances]

    if args.dry_run:
        print(f"dataset_name: {dataset_name}")
        print(f"examples: {len(examples)}")
        if examples:
            first = dict(examples[0])
            first["inputs"] = {
                **first["inputs"],
                "haystack_sessions": "<omitted in dry-run preview>",
            }
            print(first)
        return

    configure_langsmith_env(langsmith_config)
    Client = _load_langsmith_client()
    client = Client()

    if client.has_dataset(dataset_name=dataset_name):
        dataset = client.read_dataset(dataset_name=dataset_name)
    else:
        dataset = client.create_dataset(
            dataset_name=dataset_name,
            description=args.description,
        )

    client.create_examples(
        dataset_id=dataset.id,
        examples=examples,
    )
    print(f"uploaded {len(examples)} examples to LangSmith dataset: {dataset_name}")


if __name__ == "__main__":
    main()
