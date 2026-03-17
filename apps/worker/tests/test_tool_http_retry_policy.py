import unittest
from unittest.mock import patch

import httpx
from temporalio.exceptions import ApplicationError

from activities import _execute_tool_plans


class _FakeAsyncClient:
    def __init__(self, response: httpx.Response):
        self._response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, *args, **kwargs):
        _ = (args, kwargs)
        return self._response


class ToolHttpRetryPolicyTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _response(status_code: int, body: dict) -> httpx.Response:
        request = httpx.Request("POST", "http://api/internal/tool-gateway/execute")
        return httpx.Response(status_code=status_code, json=body, request=request)

    async def test_http_500_is_retryable(self) -> None:
        response = self._response(502, {"detail": {"reason_code": "adapter_http_5xx"}})
        with patch("activities.httpx.AsyncClient", return_value=_FakeAsyncClient(response)):
            with patch("activities.worker_repo.insert_cost") as insert_cost:
                with self.assertRaises(ApplicationError) as ctx:
                    await _execute_tool_plans(
                        tenant_id="default",
                        task_id="task-1",
                        run_id="run-1",
                        task_type="tool_flow",
                        user_id="user-1",
                        trace_id="trace-1",
                        tool_plans=[{"tool_id": "internal_rest_api", "payload": {"method": "GET", "path": "/records"}}],
                        approval_id=None,
                        step_key="execute_tools_activity",
                    )
        self.assertFalse(ctx.exception.non_retryable)
        insert_cost.assert_not_called()

    async def test_http_400_is_non_retryable(self) -> None:
        response = self._response(400, {"detail": {"reason_code": "adapter_http_4xx"}})
        with patch("activities.httpx.AsyncClient", return_value=_FakeAsyncClient(response)):
            with patch("activities.worker_repo.insert_cost") as insert_cost:
                with self.assertRaises(ApplicationError) as ctx:
                    await _execute_tool_plans(
                        tenant_id="default",
                        task_id="task-1",
                        run_id="run-1",
                        task_type="tool_flow",
                        user_id="user-1",
                        trace_id="trace-1",
                        tool_plans=[{"tool_id": "internal_rest_api", "payload": {"method": "GET", "path": "/records"}}],
                        approval_id=None,
                        step_key="execute_tools_activity",
                    )
        self.assertTrue(ctx.exception.non_retryable)
        insert_cost.assert_not_called()

    async def test_idempotency_in_progress_http_429_is_retryable(self) -> None:
        response = self._response(429, {"detail": {"reason_code": "idempotency_in_progress"}})
        with patch("activities.httpx.AsyncClient", return_value=_FakeAsyncClient(response)):
            with patch("activities.worker_repo.insert_cost") as insert_cost:
                with self.assertRaises(ApplicationError) as ctx:
                    await _execute_tool_plans(
                        tenant_id="default",
                        task_id="task-1",
                        run_id="run-1",
                        task_type="tool_flow",
                        user_id="user-1",
                        trace_id="trace-1",
                        tool_plans=[{"tool_id": "internal_rest_api", "payload": {"method": "GET", "path": "/records"}}],
                        approval_id=None,
                        step_key="execute_tools_activity",
                    )
        self.assertFalse(ctx.exception.non_retryable)
        insert_cost.assert_not_called()


if __name__ == "__main__":
    unittest.main()
