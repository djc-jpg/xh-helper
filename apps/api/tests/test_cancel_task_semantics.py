import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.deps import get_current_user
from app.main import app, task_repo
from app.security import create_access_token


class CancelTaskSemanticsTests(unittest.TestCase):
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
        token = create_access_token(
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "tenant_id": "default",
                "email": "operator@example.com",
                "role": "operator",
            }
        )
        self.headers = {"Authorization": f"Bearer {token}"}

    def tearDown(self) -> None:
        app.dependency_overrides.clear()
        self._ensure_schema_compat_patcher.stop()
        self._close_pool_patcher.stop()
        self._init_pool_patcher.stop()

    def test_cancel_returns_502_when_workflow_cancel_fails(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_task_by_id",
                return_value={
                    "id": "task-1",
                    "tenant_id": "default",
                    "status": "RUNNING",
                    "created_by": "00000000-0000-0000-0000-000000000001",
                },
            ),
            patch.object(
                task_repo,
                "get_latest_run_for_task",
                return_value={"id": "run-1", "workflow_id": "wf-1"},
            ),
            patch("app.main.cancel_workflow", side_effect=RuntimeError("temporal down")),
            patch.object(task_repo, "update_run_status") as update_run_status,
            patch.object(task_repo, "update_task_status") as update_task_status,
            patch.object(task_repo, "append_step") as append_step,
            patch.object(task_repo, "insert_audit_log") as insert_audit_log,
        ):
            resp = self.client.post("/tasks/task-1/cancel", headers=self.headers)

        self.assertEqual(502, resp.status_code)
        update_run_status.assert_not_called()
        update_task_status.assert_not_called()
        append_step.assert_called_once()
        insert_audit_log.assert_called_once()

    def test_cancel_rejects_terminal_task_state(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_task_by_id",
                return_value={
                    "id": "task-1",
                    "tenant_id": "default",
                    "status": "SUCCEEDED",
                    "created_by": "00000000-0000-0000-0000-000000000001",
                },
            ),
            patch.object(task_repo, "get_latest_run_for_task") as get_latest_run_for_task,
        ):
            resp = self.client.post("/tasks/task-1/cancel", headers=self.headers)

        self.assertEqual(409, resp.status_code)
        self.assertIn("terminal status", resp.text.lower())
        get_latest_run_for_task.assert_not_called()


if __name__ == "__main__":
    unittest.main()
