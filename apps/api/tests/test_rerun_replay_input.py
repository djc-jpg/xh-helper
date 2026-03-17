import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.deps import get_current_user
from app.input_crypto import encrypt_input_payload
from app.main import app, task_repo
from app.replay_input import NON_REPLAYABLE_INPUT_SENTINEL
from app.security import create_access_token


class RerunReplayInputTests(unittest.TestCase):
    def setUp(self) -> None:
        self._init_pool_patcher = patch("app.main.init_pool", return_value=None)
        self._close_pool_patcher = patch("app.main.close_pool", return_value=None)
        self._ensure_schema_compat_patcher = patch("app.main.ensure_schema_compat", return_value=None)
        self._init_pool_patcher.start()
        self._close_pool_patcher.start()
        self._ensure_schema_compat_patcher.start()
        self.client = TestClient(app)
        app.dependency_overrides[get_current_user] = lambda: {
            "id": "00000000-0000-0000-0000-000000000001",
            "tenant_id": "default",
            "email": "operator@example.com",
            "role": "operator",
            "is_active": True,
        }

    def tearDown(self) -> None:
        app.dependency_overrides.clear()
        self._ensure_schema_compat_patcher.stop()
        self._close_pool_patcher.stop()
        self._init_pool_patcher.stop()

    def test_rerun_uses_decrypted_original_input(self) -> None:
        original_input = {"question": "what is incident response", "content": "sensitive body"}
        task = {
            "id": "task-1",
            "tenant_id": "default",
            "created_by": "00000000-0000-0000-0000-000000000001",
            "task_type": "rag_qa",
            "budget": 1.0,
            "requires_hitl": False,
            "input_masked": {"question": "what is incident response", "content": "***"},
            "input_raw_encrypted": encrypt_input_payload(original_input),
        }
        token = create_access_token(
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "tenant_id": "default",
                "email": "operator@example.com",
                "role": "operator",
            }
        )
        with (
            patch.object(task_repo, "get_task_by_id", return_value=task),
            patch.object(task_repo, "get_latest_run_for_task", return_value={"status": "SUCCEEDED"}),
            patch.object(task_repo, "get_max_run_no", return_value=1),
            patch.object(task_repo, "create_run", return_value={"id": "run-2"}),
            patch.object(task_repo, "update_task_status"),
            patch.object(task_repo, "append_step"),
            patch.object(task_repo, "insert_audit_log") as insert_audit_log,
            patch("app.main.start_task_workflow") as start_workflow,
        ):
            resp = self.client.post(
                "/tasks/task-1/rerun",
                headers={"Authorization": f"Bearer {token}"},
            )
        self.assertEqual(200, resp.status_code)
        insert_audit_log.assert_called_once()
        args, kwargs = start_workflow.call_args
        payload = kwargs.get("payload") if kwargs else args[1]
        self.assertEqual(original_input, payload["input"])
        self.assertNotEqual(task["input_masked"], payload["input"])

    def test_rerun_rejects_non_replayable_marker(self) -> None:
        task = {
            "id": "task-1",
            "tenant_id": "default",
            "created_by": "00000000-0000-0000-0000-000000000001",
            "task_type": "rag_qa",
            "budget": 1.0,
            "requires_hitl": False,
            "input_masked": {"question": "what is incident response", "content": "***"},
            "input_raw_encrypted": NON_REPLAYABLE_INPUT_SENTINEL,
        }
        token = create_access_token(
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "tenant_id": "default",
                "email": "operator@example.com",
                "role": "operator",
            }
        )
        with (
            patch.object(task_repo, "get_task_by_id", return_value=task),
            patch.object(task_repo, "create_run") as create_run,
            patch("app.main.start_task_workflow") as start_workflow,
        ):
            resp = self.client.post(
                "/tasks/task-1/rerun",
                headers={"Authorization": f"Bearer {token}"},
            )
        self.assertEqual(409, resp.status_code)
        self.assertIn("non-replayable", resp.text.lower())
        create_run.assert_not_called()
        start_workflow.assert_not_called()


if __name__ == "__main__":
    unittest.main()
