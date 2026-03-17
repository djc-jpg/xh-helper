import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.deps import get_current_user
from app.input_crypto import encrypt_input_payload
from app.main import app, task_repo
from app.security import create_access_token


class _UniqueViolation(Exception):
    sqlstate = "23505"


class TaskRerunConcurrencyConflictTests(unittest.TestCase):
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

    def _task(self) -> dict:
        return {
            "id": "task-1",
            "tenant_id": "default",
            "created_by": "00000000-0000-0000-0000-000000000001",
            "task_type": "rag_qa",
            "budget": 1.0,
            "requires_hitl": False,
            "input_raw_encrypted": encrypt_input_payload({"question": "q"}),
        }

    def test_rerun_retries_on_run_no_conflict(self) -> None:
        with (
            patch.object(task_repo, "get_task_by_id", return_value=self._task()),
            patch.object(task_repo, "get_latest_run_for_task", return_value={"status": "SUCCEEDED"}),
            patch.object(task_repo, "get_max_run_no", side_effect=[1, 2, 3]),
            patch.object(task_repo, "create_run", side_effect=[_UniqueViolation("dup"), {"id": "run-3"}]) as create_run,
            patch.object(task_repo, "update_task_status"),
            patch.object(task_repo, "append_step"),
            patch.object(task_repo, "insert_audit_log"),
            patch("app.main.start_task_workflow"),
        ):
            resp = self.client.post("/tasks/task-1/rerun", headers=self.headers)
        self.assertEqual(200, resp.status_code)
        self.assertGreaterEqual(create_run.call_count, 2)

    def test_rerun_returns_409_after_repeated_conflicts(self) -> None:
        with (
            patch.object(task_repo, "get_task_by_id", return_value=self._task()),
            patch.object(task_repo, "get_latest_run_for_task", return_value={"status": "SUCCEEDED"}),
            patch.object(task_repo, "get_max_run_no", return_value=1),
            patch.object(task_repo, "create_run", side_effect=[_UniqueViolation("dup")] * 5),
            patch.object(task_repo, "update_task_status"),
            patch.object(task_repo, "append_step"),
            patch("app.main.start_task_workflow"),
        ):
            resp = self.client.post("/tasks/task-1/rerun", headers=self.headers)
        self.assertEqual(409, resp.status_code)

    def test_rerun_returns_409_when_previous_run_is_in_progress(self) -> None:
        with (
            patch("app.services.task_service.settings.rerun_conflict_test_mode", True),
            patch.object(task_repo, "get_task_by_id", return_value=self._task()),
            patch.object(task_repo, "get_latest_run_for_task", return_value={"status": "QUEUED"}),
            patch.object(task_repo, "create_run") as create_run,
            patch("app.main.start_task_workflow") as start_workflow,
        ):
            resp = self.client.post("/tasks/task-1/rerun", headers=self.headers)
        self.assertEqual(409, resp.status_code)
        create_run.assert_not_called()
        start_workflow.assert_not_called()

    def test_rerun_workflow_start_failure_records_error_code_and_message(self) -> None:
        with (
            patch.object(task_repo, "get_task_by_id", return_value=self._task()),
            patch.object(task_repo, "get_latest_run_for_task", return_value={"status": "SUCCEEDED"}),
            patch.object(task_repo, "get_max_run_no", return_value=1),
            patch.object(task_repo, "create_run", return_value={"id": "run-2"}),
            patch.object(task_repo, "update_task_status"),
            patch.object(task_repo, "append_step"),
            patch.object(task_repo, "insert_audit_log") as insert_audit_log,
            patch.object(task_repo, "mark_task_failed") as mark_task_failed,
            patch.object(task_repo, "update_run_status"),
            patch("app.main.start_task_workflow", side_effect=RuntimeError("temporal start failed")) as start_workflow,
        ):
            resp = self.client.post("/tasks/task-1/rerun", headers=self.headers)
        self.assertEqual(500, resp.status_code)
        self.assertEqual(1, start_workflow.call_count)
        insert_audit_log.assert_called_once()
        kwargs = mark_task_failed.call_args.kwargs
        self.assertEqual("workflow_start_failed", kwargs["error_code"])
        self.assertTrue(bool(kwargs["error_message"]))


if __name__ == "__main__":
    unittest.main()
