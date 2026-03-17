from __future__ import annotations

import argparse
import json

from .db import close_pool, execute, fetchall, init_pool
from .replay_input import NON_REPLAYABLE_INPUT_SENTINEL


def run_backfill(*, limit: int, dry_run: bool) -> dict[str, int]:
    rows = fetchall(
        """
        SELECT id, tenant_id
        FROM tasks
        WHERE COALESCE(input_raw_encrypted, '') = ''
        ORDER BY created_at ASC
        LIMIT %s
        """,
        (limit,),
    )

    updated = 0
    for row in rows:
        if not dry_run:
            execute(
                """
                UPDATE tasks
                SET input_raw_encrypted = %s, updated_at = NOW()
                WHERE tenant_id = %s
                  AND id = %s
                """,
                (
                    NON_REPLAYABLE_INPUT_SENTINEL,
                    str(row["tenant_id"]),
                    str(row["id"]),
                ),
            )
        updated += 1

    return {"candidates": len(rows), "updated": updated, "marked_non_replayable": updated}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Mark historical tasks without raw input as non-replayable."
    )
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    init_pool()
    try:
        result = run_backfill(limit=args.limit, dry_run=args.dry_run)
        print(json.dumps({"dry_run": args.dry_run, **result}, ensure_ascii=True))
    finally:
        close_pool()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
