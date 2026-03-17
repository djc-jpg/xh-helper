from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class ShadowRecord:
    run_id: str
    task_id: str
    timestamp: float
    actual_status: str
    predicted_status: str
    comparable: bool
    consistent: bool


def _parse_record(raw: dict[str, Any]) -> ShadowRecord | None:
    try:
        return ShadowRecord(
            run_id=str(raw.get("run_id") or ""),
            task_id=str(raw.get("task_id") or ""),
            timestamp=float(raw.get("timestamp") or 0.0),
            actual_status=str(raw.get("actual_status") or ""),
            predicted_status=str(raw.get("predicted_status") or ""),
            comparable=bool(raw.get("comparable")),
            consistent=bool(raw.get("consistent")),
        )
    except Exception:
        return None


def _load_records(artifact_dir: Path) -> list[ShadowRecord]:
    records: list[ShadowRecord] = []
    if not artifact_dir.exists():
        return records
    for path in sorted(artifact_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        record = _parse_record(payload)
        if record is not None:
            records.append(record)
    return records


def _utc_day(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def main() -> int:
    parser = argparse.ArgumentParser(description="Check MAS shadow mode consistency over a rolling window.")
    parser.add_argument("--artifact-dir", default="artifacts/shadow_mode", help="Directory of shadow artifacts.")
    parser.add_argument("--days", type=int, default=7, help="Rolling window days.")
    parser.add_argument("--min-consistency", type=float, default=0.95, help="Minimum consistency ratio [0,1].")
    parser.add_argument("--min-comparable", type=int, default=50, help="Minimum comparable samples required.")
    args = parser.parse_args()

    artifact_dir = Path(args.artifact_dir)
    records = _load_records(artifact_dir)
    if not records:
        print(f"FAIL no records found under {artifact_dir}")
        return 2

    now = time.time()
    window_start = now - max(1, int(args.days)) * 86400
    window_records = [r for r in records if r.timestamp >= window_start]
    if not window_records:
        print(f"FAIL no records in last {args.days} days")
        return 2

    covered_days = sorted({_utc_day(r.timestamp) for r in window_records})
    comparable = [r for r in window_records if r.comparable]
    consistent = [r for r in comparable if r.consistent]
    consistency = (len(consistent) / len(comparable)) if comparable else 0.0

    by_day: dict[str, list[ShadowRecord]] = defaultdict(list)
    for rec in window_records:
        by_day[_utc_day(rec.timestamp)].append(rec)

    print("Shadow Mode 7d Check")
    print(f"artifact_dir={artifact_dir}")
    print(f"window_days={args.days}")
    print(f"records_total={len(window_records)}")
    print(f"days_covered={len(covered_days)} ({', '.join(covered_days)})")
    print(f"comparable={len(comparable)}")
    print(f"consistent={len(consistent)}")
    print(f"consistency_ratio={consistency:.4f}")
    print("daily_breakdown:")
    for day in covered_days:
        rows = by_day[day]
        c_rows = [r for r in rows if r.comparable]
        c_ok = [r for r in c_rows if r.consistent]
        ratio = (len(c_ok) / len(c_rows)) if c_rows else 0.0
        print(f"  {day} total={len(rows)} comparable={len(c_rows)} consistent={len(c_ok)} ratio={ratio:.4f}")

    failures: list[str] = []
    if len(covered_days) < args.days:
        failures.append(f"days_covered<{args.days}")
    if len(comparable) < args.min_comparable:
        failures.append(f"comparable<{args.min_comparable}")
    if consistency < args.min_consistency:
        failures.append(f"consistency<{args.min_consistency}")

    if failures:
        print(f"FAIL {';'.join(failures)}")
        return 2

    print("PASS shadow mode is stable and consistent in the configured window")
    return 0


if __name__ == "__main__":
    sys.exit(main())
