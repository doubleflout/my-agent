from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any


SUPPORTED_TYPES = {
    "single-session-user",
    "single-session-preference",
    "knowledge-update",
}


DEFAULT_FILES = (
    "longmemeval_oracle.json",
    "longmemeval_s_cleaned.json",
)


def _load_json_array(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON array")
    return [item for item in data if isinstance(item, dict)]


def _filter_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item
        for item in items
        if str(item.get("question_type", "")) in SUPPORTED_TYPES
    ]


def filter_file(input_path: Path, output_path: Path) -> Counter[str]:
    items = _load_json_array(input_path)
    filtered = _filter_items(items)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(filtered, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return Counter(str(item.get("question_type", "")) for item in filtered)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter LongMemEval/LangMemEval data to the question types supported "
            "by eval.longmemeval."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("dataset/LangMemEval"),
        help="Directory containing raw LongMemEval JSON files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("eval/longmemeval/dataset"),
        help="Directory for filtered JSON files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    for filename in DEFAULT_FILES:
        input_path = args.input_dir / filename
        output_path = args.output_dir / filename.replace(".json", "_supported.json")
        counts = filter_file(input_path, output_path)
        print(f"{input_path} -> {output_path}")
        print(dict(sorted(counts.items())))


if __name__ == "__main__":
    main()
