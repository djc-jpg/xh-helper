import unittest
from unittest.mock import AsyncMock, patch

from activities import mas_orchestrate_activity


class _StubCoordinator:
    def __init__(self, status: str) -> None:
        self._status = status
        self._task_id = ""

    def submit_task(self, task: dict) -> None:
        self._task_id = str(task.get("task_id") or "")

    async def run_until_idle(self, max_cycles: int = 20) -> dict:
        _ = max_cycles
        return {
            "cycles": 2,
            "pending": 0,
            "tasks": {self._task_id: {"status": self._status}},
        }


class MasGateActivityTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_returns_skipped(self) -> None:
        payload = {"task_id": "task-1", "run_id": "run-1", "task_type": "rag_qa", "budget": 1.0, "input": {}}
        with patch("activities.settings.mas_enabled", False), patch("activities.build_mas_runtime") as build_runtime:
            result = await mas_orchestrate_activity(payload)
        self.assertEqual({"enabled": False, "status": "SKIPPED"}, result)
        build_runtime.assert_not_called()

    async def test_enabled_returns_runtime_status(self) -> None:
        payload = {
            "task_id": "task-2",
            "run_id": "run-2",
            "task_type": "tool_flow",
            "budget": 2.0,
            "input": {"estimated_cost": 0.3, "estimated_minutes": 2},
        }
        coordinator = _StubCoordinator(status="SUCCEEDED")
        with (
            patch("activities.settings.mas_enabled", True),
            patch("activities.settings.mas_orchestration_mode", "gate"),
            patch("activities.build_mas_runtime", new=AsyncMock(return_value=coordinator)),
        ):
            result = await mas_orchestrate_activity(payload)
        self.assertTrue(result["enabled"])
        self.assertEqual("gate", result["mode"])
        self.assertEqual("SUCCEEDED", result["status"])
        self.assertEqual("task-2", result["task_id"])
        self.assertEqual("run-2", result["run_id"])

    async def test_enabled_runtime_error_returns_retryable(self) -> None:
        payload = {"task_id": "task-3", "run_id": "run-3", "task_type": "rag_qa", "budget": 1.0, "input": {}}
        with (
            patch("activities.settings.mas_enabled", True),
            patch("activities.settings.mas_orchestration_mode", "gate"),
            patch("activities.build_mas_runtime", new=AsyncMock(side_effect=RuntimeError("mas runtime unavailable"))),
        ):
            result = await mas_orchestrate_activity(payload)
        self.assertTrue(result["enabled"])
        self.assertEqual("gate", result["mode"])
        self.assertEqual("FAILED_RETRYABLE", result["status"])
        self.assertIn("error", result)

    async def test_closed_loop_mode_returns_structured_result(self) -> None:
        payload = {
            "task_id": "task-4",
            "run_id": "run-4",
            "task_type": "research_summary",
            "budget": 1.0,
            "input": {"query": "summarize this issue", "success_criteria": ["non_empty_output"]},
        }
        with patch("activities.settings.mas_enabled", True), patch(
            "activities.settings.mas_orchestration_mode",
            "closed_loop",
        ):
            result = await mas_orchestrate_activity(payload)

        self.assertTrue(result["enabled"])
        self.assertEqual("closed_loop_primary", result["mode"])
        self.assertEqual("SUCCEEDED", result["status"])
        self.assertIn("state", result)
        self.assertIn("protocol_messages", result)
        self.assertEqual("summarize this issue", result["agent_runtime"]["goal"]["normalized_goal"])
        self.assertEqual("SUCCEEDED", result["agent_runtime"]["status"])
        self.assertEqual("respond", result["agent_runtime"]["current_phase"])
        self.assertEqual(result["agent_runtime"], (result.get("state") or {}).get("agent_runtime"))
        metrics = dict((result.get("state") or {}).get("metrics") or {})
        self.assertEqual("langgraph", metrics.get("graph_engine"))


if __name__ == "__main__":
    unittest.main()
