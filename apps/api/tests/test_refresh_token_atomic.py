import concurrent.futures
import threading
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app, auth_repo


class RefreshTokenAtomicTests(unittest.TestCase):
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

    @staticmethod
    def _active_user() -> dict[str, str | bool]:
        return {
            "id": "00000000-0000-0000-0000-000000000001",
            "tenant_id": "default",
            "email": "user@example.com",
            "role": "user",
            "is_active": True,
        }

    def test_refresh_token_second_use_fails(self) -> None:
        consumed = {"done": False}
        lock = threading.Lock()

        def consume_once(*, tenant_id: str, token_hash: str):  # type: ignore[no-untyped-def]
            _ = (tenant_id, token_hash)
            with lock:
                if consumed["done"]:
                    return None
                consumed["done"] = True
            return {
                "id": "00000000-0000-0000-0000-000000000010",
                "user_id": "00000000-0000-0000-0000-000000000001",
            }

        with (
            patch("app.main.decode_token", return_value={"type": "refresh", "tenant_id": "default"}),
            patch.object(auth_repo, "consume_refresh_token", side_effect=consume_once),
            patch.object(auth_repo, "get_user_by_id", return_value=self._active_user()),
            patch.object(auth_repo, "store_refresh_token"),
        ):
            first = self.client.post("/auth/refresh", json={"refresh_token": "rt-1"})
            second = self.client.post("/auth/refresh", json={"refresh_token": "rt-1"})

        self.assertEqual(200, first.status_code)
        self.assertTrue(first.json().get("access_token"))
        self.assertTrue(first.json().get("refresh_token"))
        self.assertEqual(401, second.status_code)
        self.assertIn("invalid refresh token", second.text.lower())

    def test_refresh_token_concurrent_only_one_succeeds(self) -> None:
        consumed = {"done": False}
        lock = threading.Lock()

        def consume_once(*, tenant_id: str, token_hash: str):  # type: ignore[no-untyped-def]
            _ = (tenant_id, token_hash)
            with lock:
                if consumed["done"]:
                    return None
                consumed["done"] = True
            return {
                "id": "00000000-0000-0000-0000-000000000010",
                "user_id": "00000000-0000-0000-0000-000000000001",
            }

        with (
            patch("app.main.decode_token", return_value={"type": "refresh", "tenant_id": "default"}),
            patch.object(auth_repo, "consume_refresh_token", side_effect=consume_once),
            patch.object(auth_repo, "get_user_by_id", return_value=self._active_user()),
            patch.object(auth_repo, "store_refresh_token"),
        ):

            def _call_once() -> int:
                resp = self.client.post("/auth/refresh", json={"refresh_token": "rt-concurrent"})
                return int(resp.status_code)

            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                codes = list(pool.map(lambda _: _call_once(), range(32)))

        success = sum(1 for c in codes if c == 200)
        unauthorized = sum(1 for c in codes if c == 401)
        self.assertEqual(1, success, {"codes": codes})
        self.assertEqual(31, unauthorized, {"codes": codes})


if __name__ == "__main__":
    unittest.main()
