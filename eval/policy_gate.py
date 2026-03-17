from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from apps.api.app.services.policy_memory_service import compare_eval_summaries


def _load_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--active-summary", required=True)
    parser.add_argument("--candidate-summary", required=True)
    parser.add_argument("--min-success-rate", type=float, default=0.9)
    args = parser.parse_args()

    verdict = compare_eval_summaries(
        active_summary=_load_json(args.active_summary),
        candidate_summary=_load_json(args.candidate_summary),
        min_success_rate=args.min_success_rate,
    )
    print(json.dumps(verdict, ensure_ascii=True, indent=2))
    return 0 if verdict.get("passed") else 1


if __name__ == "__main__":
    raise SystemExit(main())
