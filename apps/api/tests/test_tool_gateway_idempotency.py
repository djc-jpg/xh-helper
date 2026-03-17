import unittest
from unittest.mock import patch

import httpx

from app.tool_gateway import ToolGateway


class _FakeState:
    def __init__(self) -> None:
        self.tool_calls: dict[str, dict] = {}

    def get_caller(self, tenant_id: str, caller_user_id: str):
        return {"id": caller_user_id, "tenant_id": tenant_id, "email": "operator@example.com", "role": "operator"}

    def get_manifest(self, tenant_id: str, tool_id: str, version: str | None):
        return {
            "tool_id": "internal_rest_api",
            "version": version or "v1",
            "input_schema": {
                "type": "object",
                "required": ["method", "path"],
                "properties": {
                    "method": {"type": "string"},
                    "path": {"type": "string"},
                    "body": {"type": "object"},
                },
                "additionalProperties": True,
            },
            "output_schema": {
                "type": "object",
                "required": ["status_code", "result"],
                "properties": {"status_code": {"type": "integer"}, "result": {"type": "object"}},
                "additionalProperties": True,
            },
            "rate_limit_rpm": 60,
            "run_limit": 60,
            "timeout_overall_s": 15,
            "timeout_connect_s": 2,
            "timeout_read_s": 10,
            "masking_rules": {},
        }

    def count_run_tool_calls(self, tenant_id: str, run_id: str, tool_id: str, current_tool_call_id: str) -> int:
        count = 0
        for tool_call_id, row in self.tool_calls.items():
            if tool_call_id == current_tool_call_id:
                continue
            if row["run_id"] == run_id and row["tenant_id"] == tenant_id and row["tool_id"] == tool_id:
                count += 1
        return count

    def try_start_tool_call(
        self,
        *,
        tenant_id: str,
        tool_call_id: str,
        run_id: str,
        task_id: str,
        tool_id: str,
        caller_user_id: str,
        request_masked: dict,
        trace_id: str,
    ) -> bool:
        if tool_call_id in self.tool_calls:
            return False
        self.tool_calls[tool_call_id] = {
            "tenant_id": tenant_id,
            "run_id": run_id,
            "task_id": task_id,
            "tool_id": tool_id,
            "caller_user_id": caller_user_id,
            "request_masked": request_masked,
            "response_masked": {},
            "status": "STARTED",
            "reason_code": None,
            "trace_id": trace_id,
            "idempotency_key": tool_call_id,
            "duration_ms": 0,
        }
        return True

    def load_tool_call(self, tenant_id: str, tool_call_id: str):
        row = self.tool_calls.get(tool_call_id)
        if not row:
            return None
        return {
            "status": row["status"],
            "response_masked": row["response_masked"],
            "reason_code": row["reason_code"],
        }

    def finalize_tool_call(
        self,
        *,
        tenant_id: str,
        tool_call_id: str,
        run_id: str,
        task_id: str,
        tool_id: str,
        caller_user_id: str,
        request_masked: dict,
        response_masked: dict,
        status_text: str,
        reason_code: str | None,
        trace_id: str,
        duration_ms: int,
    ) -> None:
        row = self.tool_calls.get(tool_call_id, {})
        row.update(
            {
                "tenant_id": tenant_id,
                "run_id": run_id,
                "task_id": task_id,
                "tool_id": tool_id,
                "caller_user_id": caller_user_id,
                "request_masked": request_masked,
                "response_masked": response_masked,
                "status": status_text,
                "reason_code": reason_code,
                "trace_id": trace_id,
                "idempotency_key": tool_call_id,
                "duration_ms": duration_ms,
            }
        )
        self.tool_calls[tool_call_id] = row

    def insert_audit_log(self, **kwargs) -> None:
        _ = kwargs


class _CountingGateway(ToolGateway):
    def __init__(self, repo) -> None:
        super().__init__(repo=repo)
        self.dispatch_count = 0

    async def _dispatch(self, req, manifest, timeout):  # type: ignore[override]
        self.dispatch_count += 1
        return {"status_code": 200, "result": {"ok": True}}


class _TimeoutGateway(ToolGateway):
    async def _dispatch(self, req, manifest, timeout):  # type: ignore[override]
        _ = (req, manifest, timeout)
        raise httpx.ReadTimeout("forced read timeout")


class _NetworkGateway(ToolGateway):
    async def _dispatch(self, req, manifest, timeout):  # type: ignore[override]
        _ = (manifest, timeout)
        raise httpx.ConnectError(
            "forced connection failure",
            request=httpx.Request("GET", "http://fake-internal-service/records"),
        )


class ToolGatewayIdempotencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_tool_call_id_only_dispatches_once(self) -> None:
        repo = _FakeState()
        gateway = _CountingGateway(repo=repo)
        req = {
            "tenant_id": "default",
            "tool_call_id": "deterministic-abc123",
            "task_id": "00000000-0000-0000-0000-000000000001",
            "run_id": "00000000-0000-0000-0000-000000000002",
            "task_type": "tool_flow",
            "tool_id": "internal_rest_api",
            "payload": {"method": "POST", "path": "/records", "body": {"name": "alpha"}},
            "caller_user_id": "00000000-0000-0000-0000-000000000003",
            "approval_id": "00000000-0000-0000-0000-000000000004",
            "trace_id": "trace-1",
        }

        with (
            patch("app.tool_gateway.check_tool_policy", return_value=(True, "")),
            patch("app.tool_gateway.is_tool_write_action", return_value=False),
            patch("app.tool_gateway.rate_limiter.allow", return_value=True),
        ):
            first = await gateway.execute(req)
            second = await gateway.execute(req)

        self.assertEqual("SUCCEEDED", first["status"])
        self.assertFalse(first["idempotent_hit"])
        self.assertEqual("SUCCEEDED", second["status"])
        self.assertTrue(second["idempotent_hit"])
        self.assertEqual(1, gateway.dispatch_count)

    async def test_timeout_exception_is_mapped_to_timeout_reason(self) -> None:
        repo = _FakeState()
        gateway = _TimeoutGateway(repo=repo)
        req = {
            "tenant_id": "default",
            "tool_call_id": "timeout-abc123",
            "task_id": "00000000-0000-0000-0000-000000000001",
            "run_id": "00000000-0000-0000-0000-000000000002",
            "task_type": "tool_flow",
            "tool_id": "internal_rest_api",
            "payload": {"method": "GET", "path": "/records", "params": {"q": "all"}},
            "caller_user_id": "00000000-0000-0000-0000-000000000003",
            "approval_id": "00000000-0000-0000-0000-000000000004",
            "trace_id": "trace-timeout",
        }

        with (
            patch("app.tool_gateway.check_tool_policy", return_value=(True, "")),
            patch("app.tool_gateway.is_tool_write_action", return_value=False),
            patch("app.tool_gateway.rate_limiter.allow", return_value=True),
        ):
            result = await gateway.execute(req)

        self.assertEqual("DENIED", result["status"])
        self.assertEqual("timeout", result["reason_code"])
        self.assertFalse(result["idempotent_hit"])

    async def test_transport_exception_is_mapped_to_network_reason(self) -> None:
        repo = _FakeState()
        gateway = _NetworkGateway(repo=repo)
        req = {
            "tenant_id": "default",
            "tool_call_id": "network-abc123",
            "task_id": "00000000-0000-0000-0000-000000000001",
            "run_id": "00000000-0000-0000-0000-000000000002",
            "task_type": "tool_flow",
            "tool_id": "internal_rest_api",
            "payload": {"method": "GET", "path": "/records", "params": {"q": "all"}},
            "caller_user_id": "00000000-0000-0000-0000-000000000003",
            "approval_id": "00000000-0000-0000-0000-000000000004",
            "trace_id": "trace-network",
        }

        with (
            patch("app.tool_gateway.check_tool_policy", return_value=(True, "")),
            patch("app.tool_gateway.is_tool_write_action", return_value=False),
            patch("app.tool_gateway.rate_limiter.allow", return_value=True),
        ):
            result = await gateway.execute(req)

        self.assertEqual("DENIED", result["status"])
        self.assertEqual("adapter_network_error", result["reason_code"])
        self.assertFalse(result["idempotent_hit"])


if __name__ == "__main__":
    unittest.main()
