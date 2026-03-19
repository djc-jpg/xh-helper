import unittest
import uuid
from unittest.mock import patch

from fastapi.testclient import TestClient

from app import demo_app


class DemoAppTests(unittest.TestCase):
    def setUp(self) -> None:
        demo_app.conversation_repo.rows.clear()
        demo_app.episode_repo.rows.clear()
        demo_app.turn_repo.rows.clear()
        demo_app.task_repo.tasks.clear()
        demo_app.task_repo.runs.clear()
        demo_app.task_repo.steps.clear()
        demo_app.task_repo.approvals.clear()
        demo_app.users_by_id.clear()
        demo_app.users_by_email.clear()
        demo_app.refresh_tokens.clear()
        self.client = TestClient(demo_app.app)
        email = f"demo-{uuid.uuid4().hex[:8]}@example.com"
        resp = self.client.post(
            "/auth/register",
            json={"email": email, "password": "password123"},
        )
        self.assertEqual(200, resp.status_code)
        token = resp.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {token}"}

    def _chat(self, message: str, *, mode: str = "auto", conversation_id: str | None = None, metadata: dict | None = None):
        payload = {
            "user_id": "ignored-by-server",
            "message": message,
            "mode": mode,
            "metadata": metadata or {},
        }
        if conversation_id:
            payload["conversation_id"] = conversation_id
        return self.client.post("/assistant/chat", headers=self.headers, json=payload)

    def test_workflow_chat_exposes_tasks_and_event_token(self) -> None:
        with patch("app.services.assistant_orchestration_service.qwen_client.is_enabled", return_value=False):
            resp = self.client.post(
                "/assistant/chat",
                headers=self.headers,
                json={
                    "user_id": "ignored-by-server",
                    "message": "\u8bf7\u53d1\u8d77\u4e00\u4e2a\u6301\u7eed\u6267\u884c\u4efb\u52a1\uff0c\u5e2e\u6211\u7ee7\u7eed\u8ddf\u8fdb\u8fd9\u4e2a\u95ee\u9898\uff0c\u76f4\u5230\u6709\u7ed3\u679c\u518d\u56de\u6765\u3002",
                    "mode": "auto",
                    "metadata": {},
                },
            )

        self.assertEqual(200, resp.status_code)
        payload = resp.json()
        self.assertEqual("workflow_task", payload["route"])
        self.assertEqual("task_created", payload["response_type"])
        task_id = payload["task"]["task_id"]

        list_resp = self.client.get("/tasks", headers=self.headers)
        self.assertEqual(200, list_resp.status_code)
        tasks = list_resp.json()
        self.assertEqual(1, len(tasks))
        self.assertEqual(task_id, tasks[0]["id"])

        token_resp = self.client.post(f"/events/token?task_id={task_id}", headers=self.headers)
        self.assertEqual(200, token_resp.status_code)
        token_payload = token_resp.json()
        self.assertTrue(token_payload["token"])
        self.assertEqual(300, token_payload["expires_in_sec"])

    def test_waiting_confirmation_message_stays_natural_in_demo_mode(self) -> None:
        with patch("app.services.assistant_orchestration_service.qwen_client.is_enabled", return_value=False):
            resp = self._chat("send ticket to oncall team")

        self.assertEqual(200, resp.status_code)
        payload = resp.json()
        self.assertTrue(payload["need_confirmation"])
        self.assertEqual("tool_task", payload["route"])
        self.assertEqual("approval_request", payload["turn"]["current_phase"])
        self.assertIn("\u9ad8\u98ce\u9669\u64cd\u4f5c", payload["message"])
        self.assertNotIn("need_approval", payload["message"])

    def test_customer_request_matrix_stays_reliable_in_demo_mode(self) -> None:
        with patch("app.services.assistant_orchestration_service.qwen_client.is_enabled", return_value=False):
            capability = self._chat("\u4f60\u80fd\u5e2e\u6211\u505a\u4ec0\u4e48\uff1f")
            repo_explain = self._chat("\u8fd9\u4e2a workflow runtime \u662f\u600e\u4e48\u5de5\u4f5c\u7684\uff1f\u8bf7\u7528\u4e2d\u6587\u7b80\u6d01\u8bf4\u660e\u3002")
            optimization = self._chat("\u57fa\u4e8e\u5f53\u524d\u9879\u76ee\u72b6\u6001\uff0c\u7ed9\u6211\u4e00\u4e2a\u53ef\u843d\u5730\u7684\u4f18\u5316\u65b9\u6848\uff0c\u4f18\u5148\u6309\u6536\u76ca\u6392\u5e8f\u3002")
            modules = self._chat("\u5e2e\u6211\u5b9a\u4f4d\u8fd9\u4e2a\u4ed3\u5e93\u91cc\u6700\u503c\u5f97\u5148\u770b\u7684\u5173\u952e\u6a21\u5757\uff0c\u5e76\u89e3\u91ca\u5b83\u4eec\u4e4b\u95f4\u7684\u5173\u7cfb\u3002")
            approval = self._chat("send ticket to oncall team")
            workflow = self._chat("\u8bf7\u53d1\u8d77\u4e00\u4e2a\u6301\u7eed\u6267\u884c\u4efb\u52a1\uff0c\u5e2e\u6211\u7ee7\u7eed\u8ddf\u8fdb\u8fd9\u4e2a\u95ee\u9898\uff0c\u76f4\u5230\u6709\u7ed3\u679c\u518d\u56de\u6765\u3002")
            workflow_payload = workflow.json()
            progress = self._chat(
                "\u73b0\u5728\u8fdb\u5c55\u5230\u54ea\u4e00\u6b65\u4e86\uff1f",
                conversation_id=workflow_payload["conversation_id"],
            )
            tool_failure = self._chat("search fail temporal workflow docs", mode="tool_task")

        self.assertEqual(200, capability.status_code)
        self.assertIn("\u76f4\u63a5\u56de\u7b54\u95ee\u9898", capability.json()["message"])

        self.assertEqual(200, repo_explain.status_code)
        self.assertEqual("direct_answer", repo_explain.json()["route"])
        self.assertIn("Temporal", repo_explain.json()["message"])

        self.assertEqual(200, optimization.status_code)
        self.assertIn("1.", optimization.json()["message"])
        self.assertIn("\u9ad8\u9891\u4e3b\u8def\u5f84", optimization.json()["message"])

        self.assertEqual(200, modules.status_code)
        self.assertIn("apps/api", modules.json()["message"])
        self.assertIn("runtime_backbone", modules.json()["message"])

        self.assertEqual(200, approval.status_code)
        self.assertTrue(approval.json()["need_confirmation"])
        self.assertIn("\u9700\u8981\u4f60\u5148\u786e\u8ba4", approval.json()["message"])

        self.assertEqual(200, workflow.status_code)
        self.assertEqual("workflow_task", workflow_payload["route"])
        self.assertEqual("task_created", workflow_payload["response_type"])
        self.assertTrue(workflow_payload["task"]["task_id"])

        self.assertEqual(200, progress.status_code)
        self.assertIn("\u6301\u7eed\u6267\u884c\u4efb\u52a1", progress.json()["message"])
        self.assertIn("workflow_start", progress.json()["message"])

        self.assertEqual(200, tool_failure.status_code)
        self.assertEqual("tool_task", tool_failure.json()["route"])
        self.assertIn("\u66f4\u9ad8\u6743\u9650", tool_failure.json()["message"])


if __name__ == "__main__":
    unittest.main()
