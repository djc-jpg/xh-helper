import unittest

from idempotency import build_tool_call_id


class ToolCallIdTests(unittest.TestCase):
    def test_tool_call_id_is_deterministic(self) -> None:
        plan_payload = {"method": "POST", "path": "/records", "body": {"name": "alpha", "value": "v1"}}
        left = build_tool_call_id(
            tenant_id="default",
            run_id="run-1",
            step_key="execute_tools_activity",
            tool_id="internal_rest_api",
            call_seq=1,
            plan_payload=plan_payload,
        )
        right = build_tool_call_id(
            tenant_id="default",
            run_id="run-1",
            step_key="execute_tools_activity",
            tool_id="internal_rest_api",
            call_seq=1,
            plan_payload=plan_payload,
        )
        self.assertEqual(left, right)

    def test_tool_call_id_changes_with_step_key(self) -> None:
        plan_payload = {"method": "POST", "path": "/records", "body": {"name": "alpha", "value": "v1"}}
        first = build_tool_call_id(
            tenant_id="default",
            run_id="run-1",
            step_key="execute_tools_activity",
            tool_id="internal_rest_api",
            call_seq=1,
            plan_payload=plan_payload,
        )
        second = build_tool_call_id(
            tenant_id="default",
            run_id="run-1",
            step_key="review_activity_pending_tools",
            tool_id="internal_rest_api",
            call_seq=1,
            plan_payload=plan_payload,
        )
        self.assertNotEqual(first, second)


if __name__ == "__main__":
    unittest.main()
