import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app.deps import get_current_user
from app.main import app


class AssistantApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._init_pool_patcher = patch("app.main.init_pool", return_value=None)
        self._close_pool_patcher = patch("app.main.close_pool", return_value=None)
        self._ensure_schema_compat_patcher = patch("app.main.ensure_schema_compat", return_value=None)
        self._init_pool_patcher.start()
        self._close_pool_patcher.start()
        self._ensure_schema_compat_patcher.start()
        app.dependency_overrides[get_current_user] = lambda: {
            "id": "00000000-0000-0000-0000-000000000001",
            "tenant_id": "default",
            "role": "user",
        }
        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.pop(get_current_user, None)
        self._ensure_schema_compat_patcher.stop()
        self._close_pool_patcher.stop()
        self._init_pool_patcher.stop()

    def test_rejects_mismatched_user_id(self) -> None:
        resp = self.client.post(
            "/assistant/chat",
            json={
                "user_id": "other-user",
                "conversation_id": "conv-1",
                "message": "hello",
                "mode": "direct_answer",
                "metadata": {},
            },
        )
        self.assertEqual(403, resp.status_code)

    def test_passthrough_success_response(self) -> None:
        payload = {
            "conversation_id": "conv-1",
            "route": "direct_answer",
            "response_type": "direct_answer",
            "message": "ok",
            "task": None,
            "tool_result": None,
            "trace_id": "trace-1",
        }
        with patch("app.main.service_assistant_chat", AsyncMock(return_value=payload)):
            resp = self.client.post(
                "/assistant/chat",
                json={
                    "user_id": "00000000-0000-0000-0000-000000000001",
                    "conversation_id": "conv-1",
                    "message": "hello",
                    "mode": "direct_answer",
                    "metadata": {},
                },
            )
        self.assertEqual(200, resp.status_code)
        self.assertEqual("conv-1", resp.json()["conversation_id"])


if __name__ == "__main__":
    unittest.main()
