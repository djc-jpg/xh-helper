import unittest
from unittest.mock import patch

from app.backfill_replay_input import run_backfill
from app.replay_input import NON_REPLAYABLE_INPUT_SENTINEL


class BackfillReplayInputTests(unittest.TestCase):
    def test_backfill_updates_missing_rows(self) -> None:
        rows = [
            {"id": "task-1", "tenant_id": "default"},
            {"id": "task-2", "tenant_id": "default"},
        ]
        with (
            patch("app.backfill_replay_input.fetchall", return_value=rows),
            patch("app.backfill_replay_input.execute") as execute,
        ):
            result = run_backfill(limit=100, dry_run=False)
        self.assertEqual({"candidates": 2, "updated": 2, "marked_non_replayable": 2}, result)
        self.assertEqual(2, execute.call_count)
        for call in execute.call_args_list:
            params = call.args[1]
            self.assertEqual(NON_REPLAYABLE_INPUT_SENTINEL, params[0])

    def test_backfill_dry_run_does_not_write(self) -> None:
        rows = [{"id": "task-1", "tenant_id": "default"}]
        with (
            patch("app.backfill_replay_input.fetchall", return_value=rows),
            patch("app.backfill_replay_input.execute") as execute,
        ):
            result = run_backfill(limit=10, dry_run=True)
        self.assertEqual({"candidates": 1, "updated": 1, "marked_non_replayable": 1}, result)
        execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
