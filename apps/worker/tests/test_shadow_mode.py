import unittest

from apps.worker.mas.shadow import run_shadow_comparison, run_shadow_simulation


class ShadowModeTests(unittest.IsolatedAsyncioTestCase):
    async def test_shadow_simulation_predicts_succeeded_for_normal_input(self) -> None:
        payload = {
            "task_id": "t-1",
            "run_id": "r-1",
            "task_type": "tool_flow",
            "budget": 1.0,
            "input": {"query": "normal"},
        }
        result = await run_shadow_simulation(payload)
        self.assertEqual("SUCCEEDED", result["predicted_status"])

    async def test_shadow_simulation_predicts_retryable_failure_for_force_500(self) -> None:
        payload = {
            "task_id": "t-2",
            "run_id": "r-2",
            "task_type": "tool_flow",
            "budget": 1.0,
            "input": {"query": "force_500"},
        }
        result = await run_shadow_simulation(payload)
        self.assertEqual("FAILED_RETRYABLE", result["predicted_status"])

    async def test_shadow_comparison_marks_cancelled_as_non_comparable(self) -> None:
        payload = {
            "task_id": "t-3",
            "run_id": "r-3",
            "task_type": "ticket_email",
            "budget": 1.0,
            "input": {"content": "normal"},
        }
        result = await run_shadow_comparison(task_payload=payload, actual_status="CANCELLED")
        self.assertFalse(result["comparable"])
        self.assertFalse(result["consistent"])


if __name__ == "__main__":
    unittest.main()
