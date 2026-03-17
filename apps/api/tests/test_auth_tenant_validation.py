import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app, auth_repo


class AuthTenantValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._init_pool_patcher = patch("app.main.init_pool", return_value=None)
        self._close_pool_patcher = patch("app.main.close_pool", return_value=None)
        self._ensure_schema_compat_patcher = patch("app.main.ensure_schema_compat", return_value=None)
        self._init_pool_patcher.start()
        self._close_pool_patcher.start()
        self._ensure_schema_compat_patcher.start()
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self._ensure_schema_compat_patcher.stop()
        self._close_pool_patcher.stop()
        self._init_pool_patcher.stop()

    def test_register_rejects_invalid_tenant_header(self) -> None:
        resp = self.client.post(
            "/auth/register",
            headers={"X-Tenant-Id": "bad tenant!"},
            json={"email": "a@example.com", "password": "ChangeMe123!"},
        )
        self.assertEqual(400, resp.status_code)

    def test_login_rejects_invalid_tenant_header(self) -> None:
        resp = self.client.post(
            "/auth/login",
            headers={"X-Tenant-Id": "bad tenant!"},
            json={"email": "a@example.com", "password": "ChangeMe123!"},
        )
        self.assertEqual(400, resp.status_code)

    def test_register_accepts_valid_tenant_header(self) -> None:
        with (
            patch.object(auth_repo, "user_exists", return_value=False),
            patch.object(auth_repo, "count_users", return_value=0),
            patch.object(
                auth_repo,
                "create_user",
                return_value={"id": "u-1", "tenant_id": "tenant_a", "email": "a@example.com", "role": "owner"},
            ),
            patch.object(auth_repo, "store_refresh_token"),
        ):
            resp = self.client.post(
                "/auth/register",
                headers={"X-Tenant-Id": "tenant_a"},
                json={"email": "a@example.com", "password": "ChangeMe123!"},
            )
        self.assertEqual(200, resp.status_code)
        self.assertTrue(resp.json().get("access_token"))


if __name__ == "__main__":
    unittest.main()
