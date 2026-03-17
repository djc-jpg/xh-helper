import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.deps import get_current_user
from app.main import app, task_repo
from app.security import create_access_token


class TenantIsolationApiTests(unittest.TestCase):
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
            "tenant_id": "tenant-a",
            "email": "operator@a.example.com",
            "role": "operator",
            "is_active": True,
        }

    def tearDown(self) -> None:
        app.dependency_overrides.clear()
        self._ensure_schema_compat_patcher.stop()
        self._close_pool_patcher.stop()
        self._init_pool_patcher.stop()

    def test_tenant_header_mismatch_returns_403(self) -> None:
        resp = self.client.get("/tasks", headers={"X-Tenant-Id": "tenant-b"})
        self.assertEqual(403, resp.status_code)
        self.assertIn("tenant mismatch", resp.text.lower())

    def test_cross_tenant_task_read_returns_404(self) -> None:
        with patch.object(task_repo, "get_task_by_id", return_value=None):
            resp = self.client.get("/tasks/00000000-0000-0000-0000-000000000099")
        self.assertEqual(404, resp.status_code)

    def test_cross_tenant_approval_action_returns_404(self) -> None:
        token = create_access_token(
            {
                "id": "00000000-0000-0000-0000-000000000001",
                "tenant_id": "tenant-a",
                "email": "operator@a.example.com",
                "role": "operator",
            }
        )
        with patch.object(task_repo, "apply_approval_decision_with_outbox", side_effect=LookupError("approval_not_found")):
            resp = self.client.post(
                "/approvals/00000000-0000-0000-0000-000000000123/approve",
                headers={"Authorization": f"Bearer {token}"},
                json={"reason": "cross-tenant write attempt"},
            )
        self.assertEqual(404, resp.status_code)


if __name__ == "__main__":
    unittest.main()
