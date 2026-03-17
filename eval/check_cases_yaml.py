from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml


def _render_yaml_error(path: Path, exc: yaml.YAMLError) -> str:
    mark = getattr(exc, "problem_mark", None)
    problem = getattr(exc, "problem", None)
    if mark is not None:
        line = int(mark.line) + 1
        column = int(mark.column) + 1
        reason = problem or str(exc)
        return f"{path}:{line}:{column}: {reason}"
    return f"{path}: {exc}"


def _validate_shape(data: Any, path: Path) -> None:
    if not isinstance(data, dict):
        raise ValueError(f"{path}: top-level YAML must be a mapping")
    cases = data.get("cases")
    if not isinstance(cases, list):
        raise ValueError(f"{path}: key 'cases' must be a list")
    if not cases:
        raise ValueError(f"{path}: 'cases' must not be empty")

    seen_ids: set[str] = set()
    for idx, case in enumerate(cases, start=1):
        if not isinstance(case, dict):
            raise ValueError(f"{path}: case #{idx} must be a mapping")
        case_id = case.get("id")
        if not isinstance(case_id, str) or not case_id.strip():
            raise ValueError(f"{path}: case #{idx} must include non-empty string id")
        if case_id in seen_ids:
            raise ValueError(f"{path}: duplicate case id '{case_id}'")
        seen_ids.add(case_id)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate golden cases YAML syntax and basic shape.")
    parser.add_argument("path", nargs="?", default="eval/golden_cases.yaml")
    args = parser.parse_args()

    path = Path(args.path)
    try:
        raw = path.read_text(encoding="utf-8")
    except Exception as exc:
        print(f"{path}: unable to read file: {exc}", file=sys.stderr)
        return 1

    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        print(_render_yaml_error(path, exc), file=sys.stderr)
        return 1

    try:
        _validate_shape(data, path)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    case_count = len(data["cases"])
    print(f"{path}: OK ({case_count} cases)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
