import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from activities import review_activity


class ReviewQwenTests(unittest.IsolatedAsyncioTestCase):
    async def test_review_activity_uses_qwen_when_available(self) -> None:
        payload = {
            "tenant_id": "default",
            "task_id": "task-review-1",
            "run_id": "run-review-1",
            "task_type": "research_summary",
            "trace_id": "trace-review-1",
            "user_id": "user-1",
            "graph_result": {
                "plan_hash": "plan-1",
                "plan": ["research", "summarize"],
                "draft_output": "draft",
                "citations": [{"source": "doc.md", "snippet": "example"}],
            },
            "tool_results": [{"status": "SUCCEEDED", "result": {"value": "ok"}}],
            "approval": None,
        }
        with TemporaryDirectory() as tmp_dir, patch("activities.settings.artifact_dir", tmp_dir), patch(
            "activities.qwen_client.is_enabled", return_value=True
        ), patch("activities.qwen_client.chat_text", return_value="Qwen final summary"), patch(
            "activities.worker_repo.insert_cost"
        ), patch("activities.worker_repo.insert_artifact"):
            result = await review_activity(payload)
            artifact_path = Path(tmp_dir) / "runs" / "run-review-1" / "result.json"
            self.assertTrue(artifact_path.exists())

        self.assertEqual("Qwen final summary", result["output"])
        self.assertEqual("SUCCEEDED", result["agent_runtime"]["status"])
        self.assertEqual("reflect", result["agent_runtime"]["current_phase"])

    async def test_review_activity_prefers_edited_output(self) -> None:
        payload = {
            "tenant_id": "default",
            "task_id": "task-review-2",
            "run_id": "run-review-2",
            "task_type": "ticket_email",
            "trace_id": "trace-review-2",
            "user_id": "user-1",
            "graph_result": {
                "plan_hash": "plan-2",
                "plan": ["draft_reply", "wait_approval"],
                "draft_output": "draft",
                "citations": [],
            },
            "tool_results": [],
            "approval": {"decision": "APPROVED", "edited_output": "Approved human edited output"},
        }
        with TemporaryDirectory() as tmp_dir, patch("activities.settings.artifact_dir", tmp_dir), patch(
            "activities.worker_repo.insert_cost"
        ), patch("activities.worker_repo.insert_artifact"):
            result = await review_activity(payload)

        self.assertEqual("Approved human edited output", result["output"])


if __name__ == "__main__":
    unittest.main()
