import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app.config import settings
from app.main import app, gateway, task_repo


class InternalToolGatewayPolicyStatusTests(unittest.TestCase):
    def setUp(self) -> None:
        self._init_pool_patcher = patch("app.main.init_pool", return_value=None)
        self._close_pool_patcher = patch("app.main.close_pool", return_value=None)
        self._ensure_schema_compat_patcher = patch("app.main.ensure_schema_compat", return_value=None)
        self._init_pool_patcher.start()
        self._close_pool_patcher.start()
        self._ensure_schema_compat_patcher.start()
        self.client = TestClient(app)
        worker_token = (settings.worker_auth_tokens or {}).get(settings.default_worker_id, settings.internal_api_token)
        self.headers = {
            "X-Internal-Token": settings.internal_api_token,
            "X-Worker-Id": settings.default_worker_id,
            "X-Worker-Token": worker_token,
        }
        self.base_body = {
            "tenant_id": "default",
            "tool_call_id": "tool-call-1",
            "task_id": "task-1",
            "run_id": "run-1",
            "task_type": "tool_flow",
            "tool_id": "internal_rest_api",
            "payload": {"method": "POST", "path": "/records", "body": {"name": "a"}},
            "caller_user_id": "user-1",
            "approval_id": "approval-1",
            "trace_id": "trace-1",
        }

    def tearDown(self) -> None:
        self._ensure_schema_compat_patcher.stop()
        self._close_pool_patcher.stop()
        self._init_pool_patcher.stop()

    def _binding(self) -> dict[str, str]:
        return {
            "id": "run-1",
            "task_id": "task-1",
            "tenant_id": "default",
            "status": "RUNNING",
            "assigned_worker": settings.default_worker_id,
        }

    def _task(self) -> dict[str, str]:
        return {
            "id": "task-1",
            "tenant_id": "default",
            "task_type": "tool_flow",
            "created_by": "user-1",
            "status": "RUNNING",
        }

    def test_approval_binding_denied_returns_403(self) -> None:
        with (
            patch.object(task_repo, "get_run_binding_any_tenant", return_value=self._binding()),
            patch.object(task_repo, "get_task_by_id", return_value=self._task()),
            patch.object(gateway, "execute", AsyncMock(return_value={"status": "DENIED", "reason_code": "approval_invalid"})),
        ):
            resp = self.client.post("/internal/tool-gateway/execute", headers=self.headers, json=self.base_body)

        self.assertEqual(403, resp.status_code)

    def test_success_path_passthrough(self) -> None:
        with (
            patch.object(task_repo, "get_run_binding_any_tenant", return_value=self._binding()),
            patch.object(task_repo, "get_task_by_id", return_value=self._task()),
            patch.object(
                gateway,
                "execute",
                AsyncMock(
                    return_value={
                        "status": "SUCCEEDED",
                        "tool_call_id": "tool-call-1",
                        "reason_code": None,
                        "result": {"status_code": 200, "result": {}},
                        "idempotent_hit": False,
                    }
                ),
            ),
        ):
            resp = self.client.post("/internal/tool-gateway/execute", headers=self.headers, json=self.base_body)

        self.assertEqual(200, resp.status_code)
        self.assertEqual("SUCCEEDED", resp.json()["status"])

    def test_idempotency_in_progress_is_retryable_429(self) -> None:
        with (
            patch.object(task_repo, "get_run_binding_any_tenant", return_value=self._binding()),
            patch.object(task_repo, "get_task_by_id", return_value=self._task()),
            patch.object(
                gateway,
                "execute",
                AsyncMock(return_value={"status": "DENIED", "reason_code": "idempotency_in_progress"}),
            ),
        ):
            resp = self.client.post("/internal/tool-gateway/execute", headers=self.headers, json=self.base_body)

        self.assertEqual(429, resp.status_code)


if __name__ == "__main__":
    unittest.main()
