import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

import app.main as main_mod


class LifespanTests(unittest.TestCase):
    def setUp(self) -> None:
        self._init_pool_patcher = patch("app.main.init_pool", return_value=None)
        self._close_pool_patcher = patch("app.main.close_pool", return_value=None)
        self._ensure_schema_compat_patcher = patch("app.main.ensure_schema_compat", return_value=None)
        self._init_pool_patcher.start()
        self._close_pool_patcher.start()
        self._ensure_schema_compat_patcher.start()

        app = main_mod.app
        if hasattr(app.state, "observability_initialized"):
            delattr(app.state, "observability_initialized")
        if hasattr(app.state, "observability_ready"):
            delattr(app.state, "observability_ready")
        app.state.approval_dispatcher_task = None
        app.state.goal_scheduler_task = None

    def tearDown(self) -> None:
        self._ensure_schema_compat_patcher.stop()
        self._close_pool_patcher.stop()
        self._init_pool_patcher.stop()

    def test_no_legacy_on_event_handlers_after_lifespan_migration(self) -> None:
        self.assertEqual([], main_mod.app.router.on_startup)
        self.assertEqual([], main_mod.app.router.on_shutdown)

    def test_startup_without_otel_config_does_not_raise(self) -> None:
        with patch("app.main.setup_otel", side_effect=RuntimeError("collector unavailable")):
            with TestClient(main_mod.app) as client:
                resp = client.get("/healthz")
        self.assertEqual(200, resp.status_code)
        self.assertTrue(getattr(main_mod.app.state, "observability_initialized", False))
        self.assertFalse(getattr(main_mod.app.state, "observability_ready", True))

    def test_lifespan_reentry_does_not_reinitialize_observability(self) -> None:
        with patch("app.main.setup_otel", return_value=None) as setup_otel:
            with patch.object(main_mod.instrumentator, "instrument", return_value=main_mod.instrumentator) as instrument:
                with TestClient(main_mod.app) as c1:
                    self.assertEqual(200, c1.get("/healthz").status_code)
                with TestClient(main_mod.app) as c2:
                    self.assertEqual(200, c2.get("/healthz").status_code)
        self.assertEqual(1, setup_otel.call_count)
        self.assertEqual(1, instrument.call_count)

    def test_lifespan_runs_schema_compat_check(self) -> None:
        with patch("app.main.ensure_schema_compat", return_value=None) as ensure_schema:
            with TestClient(main_mod.app) as client:
                self.assertEqual(200, client.get("/healthz").status_code)
        self.assertEqual(1, ensure_schema.call_count)

    def test_goal_scheduler_starts_once_when_enabled(self) -> None:
        class _FakeTask:
            def __init__(self) -> None:
                self.cancelled = False

            def done(self) -> bool:
                return False

            def cancel(self) -> None:
                self.cancelled = True

            def __await__(self):
                async def _wait():
                    return None

                return _wait().__await__()

        task = _FakeTask()
        def _capture_task(coro):
            coro.close()
            return task
        with (
            patch("app.main._goal_scheduler_enabled", return_value=True),
            patch("app.main.run_goal_scheduler", AsyncMock()),
            patch("app.main.asyncio.create_task", side_effect=_capture_task) as create_task,
        ):
            with TestClient(main_mod.app) as client:
                self.assertEqual(200, client.get("/healthz").status_code)
        self.assertEqual(1, create_task.call_count)
        self.assertTrue(task.cancelled)


if __name__ == "__main__":
    unittest.main()
