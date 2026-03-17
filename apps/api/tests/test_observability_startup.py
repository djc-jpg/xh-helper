import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.main import app


class ObservabilityStartupTests(unittest.TestCase):
    def setUp(self) -> None:
        self._init_pool_patcher = patch("app.main.init_pool", return_value=None)
        self._close_pool_patcher = patch("app.main.close_pool", return_value=None)
        self._ensure_schema_compat_patcher = patch("app.main.ensure_schema_compat", return_value=None)
        self._init_pool_patcher.start()
        self._close_pool_patcher.start()
        self._ensure_schema_compat_patcher.start()

        if hasattr(app.state, "observability_initialized"):
            delattr(app.state, "observability_initialized")
        if hasattr(app.state, "observability_ready"):
            delattr(app.state, "observability_ready")

    def tearDown(self) -> None:
        self._ensure_schema_compat_patcher.stop()
        self._close_pool_patcher.stop()
        self._init_pool_patcher.stop()

    def test_startup_degrades_when_otel_setup_fails(self) -> None:
        with patch("app.main.setup_otel", side_effect=RuntimeError("collector unavailable")):
            with TestClient(app) as client:
                resp = client.get("/healthz")
        self.assertEqual(200, resp.status_code)
        self.assertTrue(getattr(app.state, "observability_initialized", False))
        self.assertFalse(getattr(app.state, "observability_ready", True))


if __name__ == "__main__":
    unittest.main()
