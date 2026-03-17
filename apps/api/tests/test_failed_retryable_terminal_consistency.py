import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.deps import get_optional_user
from app.main import app, task_repo
from app.repositories import TaskRepository
from app.state_machine import FINAL_STATES


class FailedRetryableTerminalConsistencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self._init_pool_patcher = patch("app.main.init_pool", return_value=None)
        self._close_pool_patcher = patch("app.main.close_pool", return_value=None)
        self._ensure_schema_compat_patcher = patch("app.main.ensure_schema_compat", return_value=None)
        self._init_pool_patcher.start()
        self._close_pool_patcher.start()
        self._ensure_schema_compat_patcher.start()
        self.client = TestClient(app)
        app.dependency_overrides[get_optional_user] = lambda: {
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

    def test_final_states_include_failed_retryable(self) -> None:
        self.assertIn("FAILED_RETRYABLE", FINAL_STATES)

    def test_update_run_status_query_treats_failed_retryable_as_terminal(self) -> None:
        repo = TaskRepository()
        with patch("app.repositories.execute", return_value=1) as execute:
            repo.update_run_status("default", "run-1", "FAILED_RETRYABLE")

        self.assertEqual(1, execute.call_count)
        query = str(execute.call_args.args[0])
        params = tuple(execute.call_args.args[1])
        self.assertIn("FAILED_RETRYABLE", query)
        self.assertEqual(("FAILED_RETRYABLE", "FAILED_RETRYABLE", "default", "run-1"), params)

    def test_sse_stream_ends_on_failed_retryable(self) -> None:
        task_payload = {
            "id": "task-1",
            "tenant_id": "default",
            "created_by": "00000000-0000-0000-0000-000000000001",
            "status": "FAILED_RETRYABLE",
            "trace_id": "trace-1",
            "updated_at": None,
        }
        with (
            patch("app.main._load_task_or_404", return_value=task_payload),
            patch.object(task_repo, "list_new_steps_for_sse", return_value=[]),
        ):
            resp = self.client.get("/events?task_id=task-1")

        self.assertEqual(200, resp.status_code)
        self.assertIn("event: status", resp.text)
        self.assertIn("FAILED_RETRYABLE", resp.text)
        self.assertIn("event: done", resp.text)


if __name__ == "__main__":
    unittest.main()
