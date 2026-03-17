import unittest
from unittest.mock import patch

from app.policy import check_tool_policy


class ToolGatewayApprovalBindingTests(unittest.TestCase):
    def _caller(self) -> dict[str, str]:
        return {
            "id": "00000000-0000-0000-0000-000000000003",
            "tenant_id": "default",
            "email": "operator@example.com",
            "role": "operator",
        }

    @patch("app.policy.fetchall")
    @patch("app.policy.fetchone")
    def test_rejects_cross_task_or_run_approval(self, fetchone, fetchall) -> None:
        fetchall.return_value = [
            {
                "effect": "allow",
                "role_min": "operator",
                "task_type": None,
                "tool_id": "internal_rest_api",
                "environment": "local",
                "is_write_action": True,
                "requires_approval": True,
            }
        ]
        fetchone.return_value = None

        allowed, reason = check_tool_policy(
            user=self._caller(),
            task_type="tool_flow",
            tool_id="internal_rest_api",
            is_write_action=True,
            approval_id="00000000-0000-0000-0000-000000000010",
            task_id="task-a",
            run_id="run-a",
            environment="local",
        )

        self.assertFalse(allowed)
        self.assertEqual("approval_invalid", reason)

    @patch("app.policy.fetchall")
    @patch("app.policy.fetchone")
    def test_rejects_missing_approval_for_write(self, fetchone, fetchall) -> None:
        fetchall.return_value = [
            {
                "effect": "allow",
                "role_min": "operator",
                "task_type": None,
                "tool_id": "internal_rest_api",
                "environment": "local",
                "is_write_action": True,
                "requires_approval": True,
            }
        ]
        fetchone.return_value = None

        allowed, reason = check_tool_policy(
            user=self._caller(),
            task_type="tool_flow",
            tool_id="internal_rest_api",
            is_write_action=True,
            approval_id=None,
            task_id="task-a",
            run_id="run-a",
            environment="local",
        )

        self.assertFalse(allowed)
        self.assertEqual("write_requires_approval", reason)

    @patch("app.policy.fetchall")
    @patch("app.policy.fetchone")
    def test_rejects_non_approved_status(self, fetchone, fetchall) -> None:
        fetchall.return_value = [
            {
                "effect": "allow",
                "role_min": "operator",
                "task_type": None,
                "tool_id": "internal_rest_api",
                "environment": "local",
                "is_write_action": True,
                "requires_approval": True,
            }
        ]
        fetchone.return_value = {"id": "approval-1", "status": "REJECTED"}

        allowed, reason = check_tool_policy(
            user=self._caller(),
            task_type="tool_flow",
            tool_id="internal_rest_api",
            is_write_action=True,
            approval_id="approval-1",
            task_id="task-a",
            run_id="run-a",
            environment="local",
        )

        self.assertFalse(allowed)
        self.assertEqual("approval_not_approved", reason)

    @patch("app.policy.fetchall")
    @patch("app.policy.fetchone")
    def test_allows_matching_approved_binding(self, fetchone, fetchall) -> None:
        fetchall.return_value = [
            {
                "effect": "allow",
                "role_min": "operator",
                "task_type": None,
                "tool_id": "internal_rest_api",
                "environment": "local",
                "is_write_action": True,
                "requires_approval": True,
            }
        ]
        fetchone.return_value = {"id": "approval-1", "status": "APPROVED"}

        allowed, reason = check_tool_policy(
            user=self._caller(),
            task_type="tool_flow",
            tool_id="internal_rest_api",
            is_write_action=True,
            approval_id="approval-1",
            task_id="task-a",
            run_id="run-a",
            environment="local",
        )

        self.assertTrue(allowed)
        self.assertEqual("ok", reason)

    @patch("app.policy.fetchall")
    @patch("app.policy.fetchone")
    def test_allows_matching_edited_binding(self, fetchone, fetchall) -> None:
        fetchall.return_value = [
            {
                "effect": "allow",
                "role_min": "operator",
                "task_type": None,
                "tool_id": "internal_rest_api",
                "environment": "local",
                "is_write_action": True,
                "requires_approval": True,
            }
        ]
        fetchone.return_value = {"id": "approval-1", "status": "EDITED"}

        allowed, reason = check_tool_policy(
            user=self._caller(),
            task_type="tool_flow",
            tool_id="internal_rest_api",
            is_write_action=True,
            approval_id="approval-1",
            task_id="task-a",
            run_id="run-a",
            environment="local",
        )

        self.assertTrue(allowed)
        self.assertEqual("ok", reason)


if __name__ == "__main__":
    unittest.main()
