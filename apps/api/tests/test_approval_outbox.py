import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

from app.services.approval_service import apply_approval_decision, dispatch_pending_approval_signals


class _FakeTaskRepo:
    def __init__(self) -> None:
        self.approvals: dict[str, dict] = {
            "approval-1": {
                "tenant_id": "default",
                "approval_id": "approval-1",
                "status": "WAITING_HUMAN",
                "edited_output": None,
                "run_id": "run-1",
                "workflow_id": "wf-1",
            }
        }
        self.outbox: dict[str, dict] = {}
        self.audit_rows: list[dict] = []

    def apply_approval_decision_with_outbox(
        self,
        *,
        tenant_id: str,
        approval_id: str,
        status_text: str,
        decided_by: str,
        reason: str | None,
        edited_output: str | None,
        signal_payload: dict,
    ) -> dict:
        _ = (decided_by, reason, edited_output)
        approval = self.approvals.get(approval_id)
        if not approval or approval["tenant_id"] != tenant_id:
            raise LookupError("approval_not_found")
        if approval["status"] != "WAITING_HUMAN" and approval["status"] != status_text:
            raise ValueError("approval_already_decided")
        idempotent = approval["status"] == status_text and (
            edited_output is None or approval.get("edited_output") == edited_output
        )
        approval["status"] = status_text
        if edited_output is not None:
            approval["edited_output"] = edited_output
        self.outbox[approval_id] = {
            "id": f"outbox-{approval_id}",
            "tenant_id": tenant_id,
            "approval_id": approval_id,
            "run_id": approval["run_id"],
            "workflow_id": approval["workflow_id"],
            "signal_payload": signal_payload,
            "status": "PENDING",
            "attempt_count": 0,
            "next_attempt_at": datetime.now(tz=timezone.utc),
        }
        return {
            "idempotent": idempotent,
            "run_id": approval["run_id"],
            "workflow_id": approval["workflow_id"],
            "outbox_id": f"outbox-{approval_id}",
            "outbox_status": self.outbox[approval_id]["status"],
        }

    def claim_next_approval_signal_outbox(self):
        for row in self.outbox.values():
            if row["status"] == "PENDING":
                row["status"] = "SENDING"
                return dict(row)
        return None

    def mark_approval_signal_sent(self, outbox_id: str) -> None:
        for row in self.outbox.values():
            if row["id"] == outbox_id:
                row["status"] = "SENT"
                return

    def mark_approval_signal_failure(self, *, outbox_id: str, error_message: str, retry_delay_s: int, max_attempts: int) -> dict:
        _ = retry_delay_s
        for row in self.outbox.values():
            if row["id"] != outbox_id:
                continue
            row["attempt_count"] += 1
            row["last_error"] = error_message
            row["status"] = "FAILED" if row["attempt_count"] >= max_attempts else "PENDING"
            return {"status": row["status"], "attempt_count": row["attempt_count"], "next_attempt_at": None}
        return {"status": "FAILED", "attempt_count": max_attempts, "next_attempt_at": None}

    def insert_audit_log(self, **kwargs) -> None:
        self.audit_rows.append(kwargs)


class _FakeGoalRepo:
    pass


class ApprovalOutboxTests(unittest.IsolatedAsyncioTestCase):
    async def test_signal_failure_keeps_outbox_retry_record(self) -> None:
        repo = _FakeTaskRepo()
        signal = AsyncMock(side_effect=RuntimeError("temporal unavailable"))

        result = await apply_approval_decision(
            action="approve",
            approval_id="approval-1",
            tenant_id="default",
            actor_user_id="user-1",
            reason="approve for test",
            edited_output=None,
            trace_id="trace-1",
            task_repo=repo,
            signal_approval=signal,
        )

        self.assertEqual("APPROVED", result["status"])
        self.assertIn("approval-1", repo.outbox)
        self.assertIn(repo.outbox["approval-1"]["status"], {"PENDING", "FAILED"})

    async def test_retry_dispatch_moves_outbox_to_sent(self) -> None:
        repo = _FakeTaskRepo()
        failing_signal = AsyncMock(side_effect=RuntimeError("first call fails"))
        await apply_approval_decision(
            action="approve",
            approval_id="approval-1",
            tenant_id="default",
            actor_user_id="user-1",
            reason="approve for test",
            edited_output=None,
            trace_id="trace-1",
            task_repo=repo,
            signal_approval=failing_signal,
        )
        self.assertIn(repo.outbox["approval-1"]["status"], {"PENDING", "FAILED"})

        succeeding_signal = AsyncMock(return_value=None)
        summary = await dispatch_pending_approval_signals(task_repo=repo, signal_approval=succeeding_signal, max_items=1)

        self.assertEqual(1, summary["sent"])
        self.assertEqual("SENT", repo.outbox["approval-1"]["status"])
        self.assertEqual(1, succeeding_signal.await_count)

    async def test_failed_outbox_is_terminal_and_not_claimed_again(self) -> None:
        repo = _FakeTaskRepo()
        failing_signal = AsyncMock(side_effect=RuntimeError("always fail"))

        with patch("app.services.approval_service.settings.approval_signal_retry_max_attempts", 1):
            await apply_approval_decision(
                action="approve",
                approval_id="approval-1",
                tenant_id="default",
                actor_user_id="user-1",
                reason="approve for test",
                edited_output=None,
                trace_id="trace-1",
                task_repo=repo,
                signal_approval=failing_signal,
            )

        self.assertEqual("FAILED", repo.outbox["approval-1"]["status"])
        succeeding_signal = AsyncMock(return_value=None)
        summary = await dispatch_pending_approval_signals(task_repo=repo, signal_approval=succeeding_signal, max_items=1)
        self.assertEqual(0, summary["processed"])
        self.assertEqual(0, summary["sent"])
        self.assertEqual(0, summary["failed"])
        self.assertEqual("FAILED", repo.outbox["approval-1"]["status"])
        self.assertEqual(0, succeeding_signal.await_count)

    async def test_edit_decision_sets_edited_status_with_edited_output(self) -> None:
        repo = _FakeTaskRepo()
        signal = AsyncMock(return_value=None)

        result = await apply_approval_decision(
            action="edit",
            approval_id="approval-1",
            tenant_id="default",
            actor_user_id="user-1",
            reason="edit for test",
            edited_output="Edited by operator",
            trace_id="trace-1",
            task_repo=repo,
            signal_approval=signal,
        )

        self.assertEqual("EDITED", result["status"])
        self.assertEqual("EDITED", repo.approvals["approval-1"]["status"])
        self.assertEqual("Edited by operator", repo.approvals["approval-1"]["edited_output"])
        self.assertEqual("APPROVED", repo.outbox["approval-1"]["signal_payload"]["decision"])
        self.assertEqual(
            "Edited by operator",
            repo.outbox["approval-1"]["signal_payload"]["edited_output"],
        )

    async def test_approval_decision_resumes_waiting_goals(self) -> None:
        repo = _FakeTaskRepo()
        signal = AsyncMock(return_value=None)
        goal_repo = _FakeGoalRepo()
        with patch("app.services.approval_service.resume_waiting_goals_for_event") as resume_goals:
            await apply_approval_decision(
                action="approve",
                approval_id="approval-1",
                tenant_id="default",
                actor_user_id="user-1",
                reason="approve for goal wake",
                edited_output=None,
                trace_id="trace-approval-wake",
                task_repo=repo,
                signal_approval=signal,
                goal_repo=goal_repo,
            )
        resume_goals.assert_called_once()
        self.assertEqual("approval", resume_goals.call_args.kwargs["event_kind"])
        self.assertEqual("approval-1", resume_goals.call_args.kwargs["event_key"])


if __name__ == "__main__":
    unittest.main()
