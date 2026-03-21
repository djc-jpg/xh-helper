import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.config import settings
from app.main import app, conversation_repo, episode_repo, goal_repo, task_repo, turn_repo


class InternalStatusGuardrailTests(unittest.TestCase):
    def setUp(self) -> None:
        self._init_pool_patcher = patch("app.main.init_pool", return_value=None)
        self._close_pool_patcher = patch("app.main.close_pool", return_value=None)
        self._ensure_schema_compat_patcher = patch("app.main.ensure_schema_compat", return_value=None)
        self._init_pool_patcher.start()
        self._close_pool_patcher.start()
        self._ensure_schema_compat_patcher.start()
        self.client = TestClient(app)
        worker_token = (settings.worker_auth_tokens or {}).get(settings.default_worker_id, settings.internal_api_token)
        self.headers = {
            "X-Internal-Token": settings.internal_api_token,
            "X-Worker-Id": settings.default_worker_id,
            "X-Worker-Token": worker_token,
        }

    def tearDown(self) -> None:
        self._ensure_schema_compat_patcher.stop()
        self._close_pool_patcher.stop()
        self._init_pool_patcher.stop()

    def test_rejects_task_run_mismatch(self) -> None:
        with patch.object(task_repo, "get_run_binding_any_tenant", return_value=None):
            resp = self.client.post(
                "/internal/tasks/task-a/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-b",
                    "status": "RUNNING",
                    "trace_id": "trace-1",
                    "status_event_id": "evt-mismatch-1",
                },
            )
        self.assertEqual(409, resp.status_code)

    def test_rejects_illegal_status_transition(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_run_binding_any_tenant",
                return_value={
                    "id": "run-1",
                    "task_id": "task-1",
                    "tenant_id": "default",
                    "status": "QUEUED",
                    "assigned_worker": settings.default_worker_id,
                },
            ),
            patch.object(task_repo, "has_status_event", return_value=False),
            patch.object(task_repo, "insert_audit_log") as insert_audit_log,
            patch("app.services.internal_service.internal_status_rejected_total.labels") as rejected_labels,
        ):
            resp = self.client.post(
                "/internal/tasks/task-1/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-1",
                    "status": "SUCCEEDED",
                    "trace_id": "trace-1",
                    "status_event_id": "evt-transition-1",
                },
            )
        self.assertEqual(409, resp.status_code)
        rejected_labels.assert_called_once_with(reason="invalid_transition")
        rejected_labels.return_value.inc.assert_called_once()
        insert_audit_log.assert_called_once()

    def test_cancelled_absorbs_non_terminal_status_event(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_run_binding_any_tenant",
                return_value={
                    "id": "run-1",
                    "task_id": "task-1",
                    "tenant_id": "default",
                    "status": "CANCELLED",
                    "assigned_worker": settings.default_worker_id,
                },
            ),
            patch.object(task_repo, "has_status_event", return_value=False),
            patch.object(task_repo, "update_task_status") as update_task_status,
            patch.object(task_repo, "update_run_status") as update_run_status,
            patch.object(task_repo, "append_step") as append_step,
            patch.object(task_repo, "mark_task_failed") as mark_task_failed,
            patch.object(task_repo, "mark_task_succeeded") as mark_task_succeeded,
            patch.object(task_repo, "insert_audit_log") as insert_audit_log,
            patch("app.services.internal_service.internal_status_ignored_total.inc") as ignored_inc,
            patch("app.services.internal_service.internal_status_rejected_total.labels") as rejected_labels,
        ):
            resp = self.client.post(
                "/internal/tasks/task-1/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-1",
                    "status": "RUNNING",
                    "trace_id": "trace-1",
                    "status_event_id": "evt-cancel-ignored-1",
                },
            )
        self.assertEqual(200, resp.status_code)
        body = resp.json()
        self.assertTrue(body["idempotent"])
        self.assertTrue(body["ignored"])
        self.assertEqual("terminal_state_absorbed", body["ignored_reason"])
        update_task_status.assert_not_called()
        update_run_status.assert_not_called()
        append_step.assert_not_called()
        mark_task_failed.assert_not_called()
        mark_task_succeeded.assert_not_called()
        ignored_inc.assert_called_once()
        rejected_labels.assert_called_once_with(reason="ignored_terminal_update")
        rejected_labels.return_value.inc.assert_called_once()
        insert_audit_log.assert_called_once()

    def test_cancelled_duplicate_status_event_remains_idempotent(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_run_binding_any_tenant",
                return_value={
                    "id": "run-1",
                    "task_id": "task-1",
                    "tenant_id": "default",
                    "status": "CANCELLED",
                    "assigned_worker": settings.default_worker_id,
                },
            ),
            patch.object(task_repo, "has_status_event", return_value=True),
            patch.object(task_repo, "update_task_status") as update_task_status,
            patch.object(task_repo, "update_run_status") as update_run_status,
            patch.object(task_repo, "append_step") as append_step,
        ):
            resp = self.client.post(
                "/internal/tasks/task-1/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-1",
                    "status": "WAITING_TOOL",
                    "trace_id": "trace-1",
                    "status_event_id": "evt-cancel-dup-1",
                },
            )
        self.assertEqual(200, resp.status_code)
        self.assertTrue(resp.json()["idempotent"])
        update_task_status.assert_not_called()
        update_run_status.assert_not_called()
        append_step.assert_not_called()

    def test_failed_status_long_error_normalizes_code_and_message(self) -> None:
        long_error = "authorization=Bearer super-secret-token " + ("x" * 6000)
        with (
            patch.object(
                task_repo,
                "get_run_binding_any_tenant",
                return_value={
                    "id": "run-1",
                    "task_id": "task-1",
                    "tenant_id": "default",
                    "status": "REVIEWING",
                    "assigned_worker": settings.default_worker_id,
                },
            ),
            patch.object(task_repo, "has_status_event", return_value=False),
            patch.object(task_repo, "append_step", return_value=True),
            patch.object(task_repo, "mark_task_failed") as mark_task_failed,
            patch.object(task_repo, "update_run_status") as update_run_status,
        ):
            resp = self.client.post(
                "/internal/tasks/task-1/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-1",
                    "status": "FAILED_RETRYABLE",
                    "trace_id": "trace-1",
                    "payload": {"error": long_error},
                    "status_event_id": "evt-fail-long-1",
                },
            )
        self.assertEqual(200, resp.status_code)
        kwargs = mark_task_failed.call_args.kwargs
        self.assertEqual("unknown_error", kwargs["error_code"])
        self.assertLessEqual(len(kwargs["error_message"]), 2048)
        self.assertIn("authorization=***", kwargs["error_message"])
        self.assertNotIn("super-secret-token", kwargs["error_message"])
        update_run_status.assert_called_once()

    def test_accepts_valid_transition(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_run_binding_any_tenant",
                return_value={
                    "id": "run-1",
                    "task_id": "task-1",
                    "tenant_id": "default",
                    "status": "QUEUED",
                    "assigned_worker": settings.default_worker_id,
                },
            ),
            patch.object(task_repo, "has_status_event", return_value=False),
            patch.object(task_repo, "get_task_by_id", return_value={"id": "task-1", "runtime_state": {}}),
            patch.object(task_repo, "update_task_status") as update_task_status,
            patch.object(task_repo, "update_run_status") as update_run_status,
            patch.object(task_repo, "update_task_runtime_state") as update_task_runtime_state,
            patch.object(task_repo, "append_step") as append_step,
        ):
            resp = self.client.post(
                "/internal/tasks/task-1/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-1",
                    "status": "VALIDATING",
                    "trace_id": "trace-1",
                    "payload": {
                        "agent_runtime": {
                            "goal": {"normalized_goal": "trace runtime"},
                            "task_state": {"current_phase": "plan"},
                        }
                    },
                    "status_event_id": "evt-valid-1",
                },
            )
        self.assertEqual(200, resp.status_code)
        update_task_status.assert_called_once()
        update_run_status.assert_called_once()
        update_task_runtime_state.assert_called_once()
        self.assertEqual("VALIDATING", update_task_runtime_state.call_args.kwargs["runtime_state"]["status"])
        append_step.assert_called_once()

    def test_cost_update_path_is_executed(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_run_binding_any_tenant",
                return_value={
                    "id": "run-1",
                    "task_id": "task-1",
                    "tenant_id": "default",
                    "status": "REVIEWING",
                    "assigned_worker": settings.default_worker_id,
                },
            ),
            patch.object(task_repo, "has_status_event", return_value=False),
            patch.object(task_repo, "mark_task_succeeded"),
            patch.object(task_repo, "update_run_status"),
            patch.object(task_repo, "append_step"),
            patch.object(task_repo, "add_task_cost") as add_task_cost,
            patch.object(task_repo, "get_task_cost", return_value=0.42) as get_task_cost,
        ):
            resp = self.client.post(
                "/internal/tasks/task-1/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-1",
                    "status": "SUCCEEDED",
                    "trace_id": "trace-1",
                    "payload": {"output": "ok"},
                    "cost": 0.42,
                    "status_event_id": "evt-cost-1",
                },
            )
        self.assertEqual(200, resp.status_code)
        add_task_cost.assert_called_once()
        get_task_cost.assert_called_once()

    def test_duplicate_status_event_is_idempotent(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_run_binding_any_tenant",
                return_value={
                    "id": "run-1",
                    "task_id": "task-1",
                    "tenant_id": "default",
                    "status": "REVIEWING",
                    "assigned_worker": settings.default_worker_id,
                },
            ),
            patch.object(task_repo, "has_status_event", return_value=True),
            patch.object(task_repo, "mark_task_succeeded") as mark_task_succeeded,
            patch.object(task_repo, "update_run_status") as update_run_status,
            patch.object(task_repo, "append_step") as append_step,
            patch.object(task_repo, "add_task_cost") as add_task_cost,
        ):
            resp = self.client.post(
                "/internal/tasks/task-1/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-1",
                    "status": "SUCCEEDED",
                    "trace_id": "trace-1",
                    "payload": {"output": "ok"},
                    "cost": 0.42,
                    "status_event_id": "evt-cost-dup",
                },
            )
        self.assertEqual(200, resp.status_code)
        self.assertTrue(resp.json()["idempotent"])
        mark_task_succeeded.assert_not_called()
        update_run_status.assert_not_called()
        append_step.assert_not_called()
        add_task_cost.assert_not_called()

    def test_final_status_resumes_waiting_task_completion_goals(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_run_binding_any_tenant",
                return_value={
                    "id": "run-1",
                    "task_id": "task-1",
                    "tenant_id": "default",
                    "status": "REVIEWING",
                    "assigned_worker": settings.default_worker_id,
                },
            ),
            patch.object(task_repo, "has_status_event", return_value=False),
            patch.object(task_repo, "append_step", return_value=True),
            patch.object(task_repo, "mark_task_succeeded"),
            patch.object(task_repo, "update_run_status"),
            patch.object(
                task_repo,
                "get_task_by_id",
                return_value={
                    "id": "task-1",
                    "created_by": "user-1",
                    "conversation_id": "conv-1",
                    "assistant_turn_id": "turn-1",
                    "goal_id": "goal-1",
                    "runtime_state": {"goal": {"goal_id": "goal-1", "normalized_goal": "ship release"}, "task_state": {}},
                },
            ),
            patch.object(task_repo, "update_task_runtime_state"),
            patch("app.services.internal_service.sync_goal_progress", return_value={"goal_id": "goal-1", "status": "ACTIVE"}),
            patch("app.services.internal_service.resume_waiting_goals_for_event") as resume_goals,
            patch.object(turn_repo, "get_turn", return_value=None),
            patch.object(conversation_repo, "update_memory", return_value=None),
            patch.object(task_repo, "list_tool_calls_for_task", return_value=[]),
            patch.object(goal_repo, "get_goal", return_value=None),
            patch.object(goal_repo, "list_goals_waiting_on_event", return_value=[]),
            patch.object(episode_repo, "upsert_episode", return_value=None),
        ):
            resp = self.client.post(
                "/internal/tasks/task-1/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-1",
                    "status": "SUCCEEDED",
                    "trace_id": "trace-1",
                    "payload": {"output": "ok"},
                    "status_event_id": "evt-goal-wake-1",
                },
            )
        self.assertEqual(200, resp.status_code)
        resume_goals.assert_called_once()
        self.assertEqual("task_completion", resume_goals.call_args.kwargs["event_kind"])
        self.assertEqual("task-1", resume_goals.call_args.kwargs["event_key"])

    def test_succeeded_status_updates_turn_message_from_structured_payload_and_conversation_history(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_run_binding_any_tenant",
                return_value={
                    "id": "run-1",
                    "task_id": "task-1",
                    "tenant_id": "default",
                    "status": "REVIEWING",
                    "assigned_worker": settings.default_worker_id,
                },
            ),
            patch.object(task_repo, "has_status_event", return_value=False),
            patch.object(task_repo, "append_step", return_value=True),
            patch.object(task_repo, "mark_task_succeeded"),
            patch.object(task_repo, "update_run_status"),
            patch.object(
                task_repo,
                "get_task_by_id",
                return_value={
                    "id": "task-1",
                    "created_by": "user-1",
                    "conversation_id": "conv-1",
                    "assistant_turn_id": "turn-1",
                    "goal_id": None,
                    "updated_at": "2026-03-20T12:00:00Z",
                    "runtime_state": {"task_state": {}},
                },
            ),
            patch.object(task_repo, "update_task_runtime_state"),
            patch.object(turn_repo, "get_turn", return_value={"turn_id": "turn-1", "route": "workflow_task", "response_type": "task_created", "assistant_message": "等待确认"}),
            patch.object(turn_repo, "update_turn") as update_turn,
            patch.object(conversation_repo, "upsert_message_for_turn", return_value=[] ) as upsert_message_for_turn,
            patch.object(conversation_repo, "update_memory", return_value=None),
            patch.object(task_repo, "list_tool_calls_for_task", return_value=[]),
            patch.object(episode_repo, "upsert_episode", return_value=None),
        ):
            resp = self.client.post(
                "/internal/tasks/task-1/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-1",
                    "status": "SUCCEEDED",
                    "trace_id": "trace-1",
                    "payload": {
                        "result": {
                            "summary": "已成功创建并发送值班工单。"
                        }
                    },
                    "status_event_id": "evt-structured-success-1",
                },
            )
        self.assertEqual(200, resp.status_code)
        self.assertEqual("已成功创建并发送值班工单。", update_turn.call_args.kwargs["assistant_message"])
        upsert_message_for_turn.assert_called_once()
        self.assertEqual("已成功创建并发送值班工单。", upsert_message_for_turn.call_args.kwargs["message"])

    def test_succeeded_status_localizes_known_english_completion_for_chinese_request(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_run_binding_any_tenant",
                return_value={
                    "id": "run-1",
                    "task_id": "task-1",
                    "tenant_id": "default",
                    "status": "REVIEWING",
                    "assigned_worker": settings.default_worker_id,
                },
            ),
            patch.object(task_repo, "has_status_event", return_value=False),
            patch.object(task_repo, "append_step", return_value=True),
            patch.object(task_repo, "mark_task_succeeded"),
            patch.object(task_repo, "update_run_status"),
            patch.object(
                task_repo,
                "get_task_by_id",
                return_value={
                    "id": "task-1",
                    "created_by": "user-1",
                    "conversation_id": "conv-1",
                    "assistant_turn_id": "turn-1",
                    "goal_id": None,
                    "updated_at": "2026-03-21T09:00:00Z",
                    "input_masked": {"message": "帮我给值班团队发工单"},
                    "runtime_state": {"task_state": {}},
                },
            ),
            patch.object(task_repo, "update_task_runtime_state"),
            patch.object(
                turn_repo,
                "get_turn",
                return_value={
                    "turn_id": "turn-1",
                    "route": "workflow_task",
                    "response_type": "task_created",
                    "assistant_message": "等待确认",
                    "user_message": "帮我给值班团队发工单",
                },
            ),
            patch.object(turn_repo, "update_turn") as update_turn,
            patch.object(conversation_repo, "upsert_message_for_turn", return_value=[]) as upsert_message_for_turn,
            patch.object(conversation_repo, "update_memory", return_value=None),
            patch.object(task_repo, "list_tool_calls_for_task", return_value=[]),
            patch.object(episode_repo, "upsert_episode", return_value=None),
        ):
            resp = self.client.post(
                "/internal/tasks/task-1/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-1",
                    "status": "SUCCEEDED",
                    "trace_id": "trace-1",
                    "payload": {
                        "output": "The email_ticketing workflow has been successfully initiated and approved. Approval ID: 4a005800-c027-4df1-a3c3-8c637a40b699."
                    },
                    "status_event_id": "evt-localized-success-1",
                },
            )
        self.assertEqual(200, resp.status_code)
        self.assertEqual(
            "这一步已经执行完成，邮件工单流程和工具调用都已成功完成。审批单号：4a005800-c027-4df1-a3c3-8c637a40b699。",
            update_turn.call_args.kwargs["assistant_message"],
        )
        self.assertEqual(
            "这一步已经执行完成，邮件工单流程和工具调用都已成功完成。审批单号：4a005800-c027-4df1-a3c3-8c637a40b699。",
            upsert_message_for_turn.call_args.kwargs["message"],
        )

    def test_cancelled_preempted_goal_is_translated_into_waiting_runtime(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_run_binding_any_tenant",
                return_value={
                    "id": "run-1",
                    "task_id": "task-1",
                    "tenant_id": "default",
                    "status": "RUNNING",
                    "assigned_worker": settings.default_worker_id,
                },
            ),
            patch.object(task_repo, "has_status_event", return_value=False),
            patch.object(task_repo, "append_step", return_value=True),
            patch.object(task_repo, "update_task_status"),
            patch.object(task_repo, "update_run_status"),
            patch.object(
                task_repo,
                "get_task_by_id",
                return_value={
                    "id": "task-1",
                    "created_by": "user-1",
                    "conversation_id": "conv-1",
                    "assistant_turn_id": "turn-1",
                    "goal_id": "goal-1",
                    "runtime_state": {
                        "goal": {"goal_id": "goal-1", "normalized_goal": "routine follow-up"},
                        "current_action": {"action_type": "workflow_call"},
                        "task_state": {},
                        "policy": {"selected_action": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                    },
                },
            ),
            patch.object(task_repo, "update_task_runtime_state") as update_task_runtime_state,
            patch.object(
                goal_repo,
                "get_goal",
                return_value={
                    "goal_id": "goal-1",
                    "goal_state": {
                        "portfolio": {
                            "hold_status": "PREEMPTING",
                            "preempted_task_id": "task-1",
                            "held_by_goal_id": "goal-urgent",
                            "hold_reason": "soft_preempted_by_urgent_goal",
                        }
                    },
                },
            ),
            patch("app.services.internal_service.sync_goal_progress", return_value={"goal_id": "goal-1", "status": "WAITING"}),
            patch("app.services.internal_service.resume_waiting_goals_for_event"),
            patch.object(turn_repo, "get_turn", return_value=None),
            patch.object(conversation_repo, "update_memory", return_value=None),
            patch.object(task_repo, "list_tool_calls_for_task", return_value=[]),
            patch.object(episode_repo, "upsert_episode", return_value=None),
            patch.object(goal_repo, "list_goals_waiting_on_event", return_value=[]),
        ):
            resp = self.client.post(
                "/internal/tasks/task-1/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-1",
                    "status": "CANCELLED",
                    "trace_id": "trace-1",
                    "payload": {"reason_code": "goal_preempted"},
                    "status_event_id": "evt-goal-preempt-1",
                },
            )
        self.assertEqual(200, resp.status_code)
        runtime = update_task_runtime_state.call_args.kwargs["runtime_state"]
        self.assertEqual("wait", runtime["current_action"]["action_type"])
        self.assertEqual("goal_preempted", runtime["task_state"]["latest_result"]["reason_code"])
        self.assertEqual("PREEMPTING", runtime["portfolio"]["hold_status"])

    def test_preempted_goal_success_records_portfolio_recovery_feedback(self) -> None:
        with (
            patch.object(
                task_repo,
                "get_run_binding_any_tenant",
                return_value={
                    "id": "run-1",
                    "task_id": "task-1",
                    "tenant_id": "default",
                    "status": "REVIEWING",
                    "assigned_worker": settings.default_worker_id,
                },
            ),
            patch.object(task_repo, "has_status_event", return_value=False),
            patch.object(task_repo, "append_step", return_value=True),
            patch.object(task_repo, "mark_task_succeeded"),
            patch.object(task_repo, "update_run_status"),
            patch.object(
                task_repo,
                "get_task_by_id",
                return_value={
                    "id": "task-1",
                    "created_by": "user-1",
                    "conversation_id": "conv-1",
                    "assistant_turn_id": "turn-1",
                    "goal_id": "goal-1",
                    "runtime_state": {
                        "goal": {"goal_id": "goal-1", "normalized_goal": "resume deferred audit"},
                        "current_action": {"action_type": "workflow_call"},
                        "task_state": {},
                        "policy": {
                            "selected_action": "workflow_call",
                            "policy_version_id": "policy-active",
                            "policy_selector": {"mode": "active"},
                            "shadow_policy": {
                                "version_id": "policy-canary",
                                "action_type": "tool_call",
                                "route": "tool_task",
                            },
                        },
                        "reflection": {"next_action": "workflow_call"},
                        "portfolio": {
                            "resume_strategy": "replan_after_preemption",
                            "last_held_by_goal_id": "goal-urgent",
                            "last_hold_reason": "soft_preempted_by_urgent_goal",
                            "shadow_portfolio": {
                                "version_id": "policy-canary",
                                "shadow_selected_goal_ids": ["goal-shadow"],
                                "live_external_wait_sources": ["vendor_webhook"],
                                "shadow_external_wait_sources": [],
                                "diverged": True,
                                "high_urgency": True,
                            },
                        },
                        "agenda": {"priority_score": 0.74},
                    },
                },
            ),
            patch.object(task_repo, "update_task_runtime_state"),
            patch.object(goal_repo, "get_goal", return_value=None),
            patch("app.services.internal_service.sync_goal_progress", return_value={"goal_id": "goal-1", "status": "ACTIVE"}),
            patch("app.services.internal_service.resume_waiting_goals_for_event"),
            patch.object(turn_repo, "get_turn", return_value=None),
            patch.object(conversation_repo, "update_memory", return_value=None),
            patch.object(task_repo, "list_tool_calls_for_task", return_value=[]),
            patch.object(goal_repo, "list_goals_waiting_on_event", return_value=[]),
            patch.object(episode_repo, "upsert_episode", return_value=None),
            patch("app.services.internal_service.record_portfolio_feedback") as record_portfolio_feedback,
            patch("app.services.internal_service.record_shadow_policy_outcome") as record_shadow_policy_outcome,
            patch("app.services.internal_service.record_shadow_portfolio_outcome") as record_shadow_portfolio_outcome,
        ):
            resp = self.client.post(
                "/internal/tasks/task-1/status",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "run_id": "run-1",
                    "status": "SUCCEEDED",
                    "trace_id": "trace-1",
                    "payload": {"output": "ok"},
                    "status_event_id": "evt-goal-preempt-success-1",
                },
            )
        self.assertEqual(200, resp.status_code)
        record_portfolio_feedback.assert_called_once()
        record_shadow_policy_outcome.assert_called_once()
        record_shadow_portfolio_outcome.assert_called_once()
        self.assertEqual("preempt_resume_success", record_portfolio_feedback.call_args.kwargs["feedback"]["event_kind"])


if __name__ == "__main__":
    unittest.main()
