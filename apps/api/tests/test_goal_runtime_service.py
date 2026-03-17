import unittest
from unittest.mock import patch

from app.config import settings
from app.services.goal_runtime_service import (
    build_preempted_goal_runtime,
    resume_goal_from_event,
    resume_waiting_goals_for_event,
    sync_goal_progress,
)


class _FakeGoalRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.subgoals: dict[str, list[dict]] = {}

    def get_goal(self, *, tenant_id: str, goal_id: str):
        del tenant_id
        return self.rows.get(goal_id)

    def find_open_goal(self, *, tenant_id: str, user_id: str, conversation_id: str | None, normalized_goal: str):
        del tenant_id, user_id, conversation_id
        for row in self.rows.values():
            if row["normalized_goal"] == normalized_goal and row["status"] in {"ACTIVE", "WAITING"}:
                return row
        return None

    def create_goal(self, **kwargs):
        row = dict(kwargs)
        row["continuation_count"] = 0
        self.rows[row["goal_id"]] = row
        return row

    def update_goal(self, **kwargs):
        row = self.rows[kwargs["goal_id"]]
        row.update(kwargs)

    def replace_subgoals(self, *, tenant_id: str, goal_id: str, subgoals: list[dict]):
        del tenant_id
        self.subgoals[goal_id] = list(subgoals)

    def list_subgoals(self, *, tenant_id: str, goal_id: str):
        del tenant_id
        return list(self.subgoals.get(goal_id) or [])

    def list_goals_waiting_on_event(
        self,
        *,
        tenant_id: str,
        event_kind: str,
        event_key: str,
        user_id: str | None = None,
        conversation_id: str | None = None,
        limit: int = 10,
    ):
        del tenant_id, limit
        rows: list[dict] = []
        for row in self.rows.values():
            state = dict(row.get("goal_state") or {})
            wake = dict(state.get("wake_condition") or {})
            subscriptions = [item for item in list(state.get("event_subscriptions") or []) if isinstance(item, dict)]
            matches_primary = str(wake.get("kind") or "") == event_kind and str(wake.get("event_key") or "") == event_key
            matches_subscription = any(
                str(item.get("kind") or "") == event_kind
                and str(item.get("event_key") or "") == event_key
                and str(item.get("status") or "pending") == "pending"
                for item in subscriptions
            )
            if not matches_primary and not matches_subscription:
                continue
            if user_id and str(row.get("user_id") or "") != user_id:
                continue
            if conversation_id is not None and str(row.get("conversation_id") or "") != str(conversation_id):
                continue
            rows.append(row)
        return rows


class GoalRuntimeServiceTests(unittest.TestCase):
    def test_sync_goal_progress_reuses_open_goal_and_tracks_continuations(self) -> None:
        repo = _FakeGoalRepo()
        first = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-1",
            goal={"normalized_goal": "prepare quarterly report"},
            runtime_state={"status": "RUNNING", "current_phase": "plan", "current_action": {"action_type": "workflow_call"}},
            task_id="task-1",
            turn_id="turn-1",
        )
        second = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-1",
            goal={"normalized_goal": "prepare quarterly report", "goal_id": first["goal_id"]},
            runtime_state={"status": "WAITING_HUMAN", "current_phase": "wait", "current_action": {"action_type": "wait"}},
            task_id="task-2",
            turn_id="turn-2",
            goal_id=first["goal_id"],
        )

        self.assertEqual(first["goal_id"], second["goal_id"])
        self.assertEqual("WAITING", second["status"])
        self.assertEqual(1, second["continuation_count"])
        active_subgoal = second["goal_state"]["active_subgoal"]
        self.assertEqual("WAITING", active_subgoal["status"])
        self.assertEqual("external_signal", second["goal_state"]["wake_condition"]["kind"])
        self.assertTrue(repo.list_subgoals(tenant_id="default", goal_id=first["goal_id"]))

    def test_sync_goal_progress_builds_subgoals_from_success_criteria(self) -> None:
        repo = _FakeGoalRepo()
        synced = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-2",
            goal={
                "normalized_goal": "launch incident response",
                "success_criteria": ["Gather evidence", "Prepare summary", "Notify stakeholders"],
            },
            runtime_state={
                "status": "RUNNING",
                "current_phase": "plan",
                "current_action": {"action_type": "workflow_call"},
                "goal_ref": {"active_subgoal_index": 1},
            },
        )

        subgoals = repo.list_subgoals(tenant_id="default", goal_id=synced["goal_id"])
        self.assertEqual(3, len(subgoals))
        self.assertEqual("COMPLETED", subgoals[0]["status"])
        self.assertEqual("ACTIVE", subgoals[1]["status"])
        self.assertEqual("Gather evidence", subgoals[0]["title"])
        self.assertEqual([subgoals[0]["subgoal_id"]], subgoals[1]["depends_on"])
        self.assertEqual(1, len(synced["goal_state"]["blocked_subgoals"]))
        self.assertEqual(subgoals[2]["subgoal_id"], synced["goal_state"]["blocked_subgoals"][0]["subgoal_id"])
        self.assertEqual(subgoals[1]["subgoal_id"], synced["goal_state"]["active_subgoal"]["subgoal_id"])
        self.assertEqual(subgoals[1]["subgoal_id"], synced["goal_state"]["wake_graph"]["active_subgoal_id"])
        self.assertGreater(float(synced["goal_state"]["agenda"]["priority_score"]), 0.0)

    def test_sync_goal_progress_marks_blocked_subgoals_until_dependencies_finish(self) -> None:
        repo = _FakeGoalRepo()
        synced = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-3",
            goal={
                "normalized_goal": "coordinate launch",
                "subgoals": [
                    {"title": "Collect requirements"},
                    {"title": "Approve plan", "depends_on": ["goal-manual:sg:1"]},
                    {"title": "Execute rollout", "depends_on": ["goal-manual:sg:2"]},
                ],
                "goal_id": "goal-manual",
            },
            runtime_state={
                "status": "RUNNING",
                "current_phase": "plan",
                "current_action": {"action_type": "workflow_call"},
                "goal_ref": {"active_subgoal_id": "goal-manual:sg:1", "active_subgoal_index": 0},
            },
            goal_id="goal-manual",
        )

        subgoals = repo.list_subgoals(tenant_id="default", goal_id=synced["goal_id"])
        self.assertEqual("ACTIVE", subgoals[0]["status"])
        self.assertEqual("BLOCKED", subgoals[1]["status"])
        self.assertEqual(["goal-manual:sg:1"], subgoals[1]["depends_on"])
        self.assertEqual(["goal-manual:sg:1"], subgoals[1]["dependency_status"]["missing"])
        self.assertEqual(2, len(synced["goal_state"]["blocked_subgoals"]))

    def test_sync_goal_progress_inserts_dynamic_repair_subgoal_for_failed_replan(self) -> None:
        repo = _FakeGoalRepo()
        synced = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-4",
            goal={
                "normalized_goal": "stabilize deployment",
                "success_criteria": ["Diagnose failure", "Apply fix", "Verify recovery"],
            },
            runtime_state={
                "status": "FAILED_RETRYABLE",
                "current_phase": "reflect",
                "current_action": {"action_type": "replan"},
                "reflection": {"requires_replan": True, "summary": "Need a safer remediation path"},
                "task_state": {
                    "blockers": ["rollback procedure unclear"],
                    "unknowns": ["which release artifact failed"],
                    "latest_result": {"status": "FAILED_RETRYABLE", "reason_code": "adapter_http_5xx"},
                },
                "goal_ref": {"active_subgoal_index": 1},
            },
        )

        subgoals = repo.list_subgoals(tenant_id="default", goal_id=synced["goal_id"])
        dynamic_subgoals = [row for row in subgoals if row["kind"] == "dynamic"]
        self.assertGreaterEqual(len(dynamic_subgoals), 2)
        self.assertTrue(any("Resolve blocker:" in row["title"] for row in dynamic_subgoals))
        self.assertTrue(any("Recover and replan:" in row["title"] for row in dynamic_subgoals))
        self.assertEqual("ACTIVE", synced["goal_state"]["active_subgoal"]["status"])
        self.assertEqual("dynamic", synced["goal_state"]["active_subgoal"]["kind"])
        self.assertTrue(any(item["event_key"] for item in synced["goal_state"]["wake_graph"]["waiting_events"] if item["kind"] == "dependency"))
        self.assertEqual("urgent", synced["goal_state"]["agenda"]["priority_bucket"])
        self.assertEqual("dynamic", synced["goal_state"]["agenda"]["active_subgoal_kind"])

    def test_sync_goal_progress_tracks_task_completion_wake_condition(self) -> None:
        repo = _FakeGoalRepo()
        synced = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-5",
            goal={"normalized_goal": "wait for dependent task"},
            runtime_state={
                "status": "WAITING_HUMAN",
                "current_phase": "wait",
                "current_action": {"action_type": "wait"},
                "task_state": {"latest_result": {"awaiting_task_id": "task-dependency-1"}},
            },
        )

        self.assertEqual("task_completion", synced["goal_state"]["wake_condition"]["kind"])
        self.assertEqual("task-dependency-1", synced["goal_state"]["wake_condition"]["event_key"])

    def test_resume_goal_from_event_reactivates_waiting_goal(self) -> None:
        repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-resume",
            goal={"normalized_goal": "collect missing evidence"},
            runtime_state={
                "status": "WAITING_HUMAN",
                "current_phase": "wait",
                "current_action": {"action_type": "ask_user"},
                "reflection": {"next_action": "workflow_call"},
                "conversation_id": "conv-resume",
            },
            turn_id="turn-waiting",
        )

        resumed = resume_goal_from_event(
            repo=repo,
            tenant_id="default",
            goal_row=waiting,
            event_kind="user_message",
            event_payload={"message": "I uploaded the spreadsheet"},
        )

        self.assertEqual("ACTIVE", resumed["status"])
        self.assertEqual("scheduler_cooldown", resumed["goal_state"]["wake_condition"]["kind"])
        self.assertEqual("workflow_call", resumed["goal_state"]["current_action"]["action_type"])
        latest_result = resumed["goal_state"]["task_state"]["latest_result"]
        self.assertEqual("user_message", latest_result["event_kind"])

    def test_resume_waiting_goal_requires_all_subscriptions_before_resume(self) -> None:
        repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-composite",
            goal={
                "normalized_goal": "finalize audited response",
                "wake_requirements": [
                    {"kind": "approval", "event_key": "approval-1", "source": "approval_queue", "resume_action": "workflow_call"},
                    {"kind": "user_message", "event_key": "conv-composite", "source": "conversation", "resume_action": "workflow_call"},
                ],
            },
            runtime_state={
                "status": "WAITING_HUMAN",
                "current_phase": "wait",
                "current_action": {"action_type": "wait"},
                "reflection": {"next_action": "workflow_call"},
                "conversation_id": "conv-composite",
            },
            turn_id="turn-composite",
        )

        self.assertEqual("composite", waiting["goal_state"]["wake_condition"]["kind"])
        self.assertEqual(2, len(waiting["goal_state"]["pending_event_subscriptions"]))

        partial = resume_goal_from_event(
            repo=repo,
            tenant_id="default",
            goal_row=waiting,
            event_kind="approval",
            event_key="approval-1",
            event_payload={"approval_id": "approval-1"},
        )
        self.assertEqual("WAITING", partial["status"])
        self.assertEqual("user_message", partial["goal_state"]["wake_condition"]["kind"])
        self.assertEqual(1, len(partial["goal_state"]["pending_event_subscriptions"]))
        self.assertEqual(1, len(partial["goal_state"]["event_memory"]))

        resumed = resume_waiting_goals_for_event(
            repo=repo,
            tenant_id="default",
            event_kind="user_message",
            event_key="conv-composite",
            event_payload={"message": "approved and attached"},
            conversation_id="conv-composite",
            limit=5,
        )[0]
        self.assertEqual("ACTIVE", resumed["status"])
        self.assertEqual("scheduler_cooldown", resumed["goal_state"]["wake_condition"]["kind"])
        self.assertEqual(2, len(resumed["goal_state"]["event_memory"]))

    def test_resume_goal_from_event_requires_matching_external_entity_ref(self) -> None:
        repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-entity",
            goal={
                "normalized_goal": "wait for artifact 42",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "artifact_store:topic:artifact_ready",
                        "source": "artifact_store",
                        "event_topic": "artifact_ready",
                        "entity_refs": ["artifact-42"],
                        "resume_action": "workflow_call",
                    }
                ],
            },
            runtime_state={
                "status": "WAITING_HUMAN",
                "current_phase": "wait",
                "current_action": {"action_type": "wait"},
                "reflection": {"next_action": "workflow_call"},
            },
        )

        unmatched = resume_goal_from_event(
            repo=repo,
            tenant_id="default",
            goal_row=waiting,
            event_kind="external_signal",
            event_key="artifact_store:topic:artifact_ready",
            event_payload={
                "source": "artifact_store",
                "event_topic": "artifact_ready",
                "artifact_id": "artifact-7",
            },
        )
        self.assertEqual("WAITING", unmatched["status"])
        self.assertEqual(0, len(unmatched["goal_state"]["event_memory"]))

        matched = resume_goal_from_event(
            repo=repo,
            tenant_id="default",
            goal_row=waiting,
            event_kind="external_signal",
            event_key="artifact_store:topic:artifact_ready",
            event_payload={
                "source": "artifact_store",
                "event_topic": "artifact_ready",
                "artifact_id": "artifact-42",
            },
        )
        self.assertEqual("ACTIVE", matched["status"])
        self.assertEqual("artifact-42", matched["goal_state"]["event_memory"][0]["entity_refs"][0])

    def test_resume_goal_from_external_progress_keeps_waiting_but_records_observation(self) -> None:
        repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-progress",
            goal={
                "normalized_goal": "wait for vendor callback",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "vendor_webhook:job_id:job-42",
                        "source": "vendor_webhook",
                        "resume_action": "workflow_call",
                    }
                ],
            },
            runtime_state={
                "status": "WAITING_HUMAN",
                "current_phase": "wait",
                "current_action": {"action_type": "wait"},
                "reflection": {"next_action": "workflow_call"},
            },
        )

        self.assertEqual(
            ["success", "failure", "timeout"],
            waiting["goal_state"]["event_subscriptions"][0]["expected_outcomes"],
        )

        observed = resume_goal_from_event(
            repo=repo,
            tenant_id="default",
            goal_row=waiting,
            event_kind="external_signal",
            event_key="vendor_webhook:job_id:job-42",
            event_payload={
                "source": "vendor_webhook",
                "job_id": "job-42",
                "status": "running",
                "adapter_outcome": "progress",
                "observation_summary": "Vendor job is still running.",
            },
        )

        self.assertEqual("WAITING", observed["status"])
        self.assertEqual("external_signal", observed["goal_state"]["wake_condition"]["kind"])
        self.assertEqual(1, len(observed["goal_state"]["event_memory"]))
        self.assertEqual(1, len(observed["goal_state"]["pending_event_subscriptions"]))
        self.assertEqual("progress", observed["goal_state"]["task_state"]["latest_result"]["event_outcome"])

    def test_resume_goal_from_external_progress_can_resume_when_progress_is_expected(self) -> None:
        repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-progress-expected",
            goal={
                "normalized_goal": "wait for vendor progress checkpoint",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "vendor_webhook:job_id:job-88",
                        "source": "vendor_webhook",
                        "resume_action": "workflow_call",
                        "expected_outcomes": ["progress"],
                    }
                ],
            },
            runtime_state={
                "status": "WAITING_HUMAN",
                "current_phase": "wait",
                "current_action": {"action_type": "wait"},
                "reflection": {"next_action": "workflow_call"},
            },
        )

        resumed = resume_goal_from_event(
            repo=repo,
            tenant_id="default",
            goal_row=waiting,
            event_kind="external_signal",
            event_key="vendor_webhook:job_id:job-88",
            event_payload={
                "source": "vendor_webhook",
                "job_id": "job-88",
                "status": "running",
                "adapter_outcome": "progress",
                "observation_summary": "Vendor job entered the running state.",
            },
        )

        self.assertEqual("ACTIVE", resumed["status"])
        self.assertEqual("workflow_call", resumed["goal_state"]["current_action"]["action_type"])
        self.assertEqual([], resumed["goal_state"]["pending_event_subscriptions"])
        self.assertEqual("progress", resumed["goal_state"]["task_state"]["latest_result"]["event_outcome"])

    def test_resume_goal_from_external_failure_forces_replan(self) -> None:
        repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-failure",
            goal={
                "normalized_goal": "wait for vendor callback",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "vendor_webhook:job_id:job-9",
                        "source": "vendor_webhook",
                        "resume_action": "workflow_call",
                    }
                ],
            },
            runtime_state={
                "status": "WAITING_HUMAN",
                "current_phase": "wait",
                "current_action": {"action_type": "wait"},
                "reflection": {"next_action": "workflow_call"},
            },
        )

        resumed = resume_goal_from_event(
            repo=repo,
            tenant_id="default",
            goal_row=waiting,
            event_kind="external_signal",
            event_key="vendor_webhook:job_id:job-9",
            event_payload={
                "source": "vendor_webhook",
                "job_id": "job-9",
                "status": "failed",
                "adapter_outcome": "failure",
                "requires_replan": True,
                "observation_summary": "Observed external failure `failed` for `job-9`.",
            },
        )

        self.assertEqual("ACTIVE", resumed["status"])
        self.assertEqual("scheduler_cooldown", resumed["goal_state"]["wake_condition"]["kind"])
        self.assertEqual("replan", resumed["goal_state"]["current_action"]["action_type"])
        self.assertTrue(resumed["goal_state"]["reflection"]["requires_replan"])
        self.assertEqual("failure", resumed["goal_state"]["policy"]["resume_outcome"])
        self.assertEqual([], resumed["goal_state"]["pending_event_subscriptions"])
        self.assertEqual("failure", resumed["goal_state"]["task_state"]["latest_result"]["event_outcome"])

    def test_sync_goal_progress_marks_expired_required_subscription_for_replan(self) -> None:
        repo = _FakeGoalRepo()
        synced = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-timeout",
            goal={
                "normalized_goal": "wait for vendor callback",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "vendor-callback",
                        "source": "vendor_webhook",
                        "resume_action": "workflow_call",
                        "expires_at": "2000-01-01T00:00:00+00:00",
                    }
                ],
            },
            runtime_state={
                "status": "WAITING_HUMAN",
                "current_phase": "wait",
                "current_action": {"action_type": "wait"},
                "reflection": {"next_action": "workflow_call"},
            },
        )

        self.assertEqual("scheduler_cooldown", synced["goal_state"]["wake_condition"]["kind"])
        self.assertEqual("subscription_timeout", synced["goal_state"]["wake_condition"]["status"])
        self.assertEqual(1, synced["goal_state"]["event_timeouts"]["expired_required_count"])
        self.assertEqual("expired", synced["goal_state"]["event_subscriptions"][0]["status"])

    def test_sync_goal_progress_shortens_external_timeout_for_low_reliability_source_topic(self) -> None:
        repo = _FakeGoalRepo()
        with (
            patch.object(settings, "goal_event_subscription_default_timeout_s", 1000),
            patch.object(settings, "goal_external_source_confidence_floor", 0.25),
            patch.object(settings, "goal_external_source_low_reliability_score", -0.2),
            patch.object(settings, "goal_external_source_low_timeout_multiplier", 0.5),
        ):
            synced = sync_goal_progress(
                repo=repo,
                tenant_id="default",
                user_id="user-1",
                conversation_id="conv-source-low",
                goal={
                    "normalized_goal": "wait for vendor callback",
                    "wake_requirements": [
                        {
                            "kind": "external_signal",
                            "event_key": "vendor_webhook:job_id:job-42",
                            "source": "vendor_webhook",
                            "event_topic": "completed",
                            "required": True,
                            "resume_action": "workflow_call",
                        }
                    ],
                },
                runtime_state={
                    "status": "WAITING_HUMAN",
                    "current_phase": "wait",
                    "current_action": {"action_type": "wait"},
                    "policy": {
                        "policy_memory": {
                            "external_source_reliability": {
                                "vendor_webhook:topic:completed": {"score": -0.8, "confidence": 0.9},
                            }
                        }
                    },
                },
            )

        subscription = synced["goal_state"]["event_subscriptions"][0]
        self.assertEqual(500, subscription["timeout_s"])
        self.assertEqual("low_reliability", subscription["source_strategy_tier"])
        self.assertEqual(-0.8, subscription["source_reliability_score"])
        self.assertEqual(-0.8, synced["goal_state"]["wake_condition"]["source_reliability_score"])

    def test_sync_goal_progress_prefers_topic_reliability_over_source_default(self) -> None:
        repo = _FakeGoalRepo()
        with (
            patch.object(settings, "goal_event_subscription_default_timeout_s", 1000),
            patch.object(settings, "goal_external_source_confidence_floor", 0.25),
            patch.object(settings, "goal_external_source_high_reliability_score", 0.2),
            patch.object(settings, "goal_external_source_high_timeout_multiplier", 1.5),
        ):
            synced = sync_goal_progress(
                repo=repo,
                tenant_id="default",
                user_id="user-1",
                conversation_id="conv-source-topic",
                goal={
                    "normalized_goal": "wait for vendor callback",
                    "wake_requirements": [
                        {
                            "kind": "external_signal",
                            "event_key": "vendor_webhook:job_id:job-43",
                            "source": "vendor_webhook",
                            "event_topic": "completed",
                            "required": True,
                            "resume_action": "workflow_call",
                        }
                    ],
                },
                runtime_state={
                    "status": "WAITING_HUMAN",
                    "current_phase": "wait",
                    "current_action": {"action_type": "wait"},
                    "policy": {
                        "policy_memory": {
                            "external_source_reliability": {
                                "vendor_webhook": {"score": -0.9, "confidence": 0.95},
                                "vendor_webhook:topic:completed": {"score": 0.7, "confidence": 0.95},
                            }
                        }
                    },
                },
            )

        subscription = synced["goal_state"]["event_subscriptions"][0]
        self.assertEqual(1500, subscription["timeout_s"])
        self.assertEqual("high_reliability", subscription["source_strategy_tier"])
        self.assertEqual(0.7, subscription["source_reliability_score"])

    def test_sync_goal_progress_preserves_existing_portfolio_hold(self) -> None:
        repo = _FakeGoalRepo()
        created = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-hold",
            goal={"normalized_goal": "defer routine follow-up"},
            runtime_state={
                "status": "RUNNING",
                "current_phase": "plan",
                "current_action": {"action_type": "workflow_call"},
                "portfolio": {"hold_status": "HELD", "hold_until": "2999-01-01T00:00:00+00:00"},
            },
        )

        updated = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-hold",
            goal={"normalized_goal": "defer routine follow-up", "goal_id": created["goal_id"]},
            runtime_state={
                "status": "WAITING_HUMAN",
                "current_phase": "wait",
                "current_action": {"action_type": "wait"},
            },
            goal_id=created["goal_id"],
        )

        self.assertEqual("HELD", updated["goal_state"]["portfolio"]["hold_status"])
        self.assertEqual("2999-01-01T00:00:00+00:00", updated["goal_state"]["portfolio"]["hold_until"])

    def test_build_preempted_goal_runtime_turns_cancel_into_waiting_resume(self) -> None:
        runtime = build_preempted_goal_runtime(
            {
                "status": "CANCELLED",
                "current_phase": "reflect",
                "current_action": {"action_type": "workflow_call"},
                "task_state": {"latest_result": {"status": "CANCELLED"}},
                "policy": {"selected_action": "workflow_call"},
                "reflection": {"next_action": "workflow_call"},
            },
            goal_row={
                "goal_state": {
                    "portfolio": {
                        "hold_status": "PREEMPTING",
                        "preempted_task_id": "task-1",
                        "held_by_goal_id": "goal-urgent",
                        "hold_reason": "soft_preempted_by_urgent_goal",
                    }
                }
            },
            task_id="task-1",
        )

        self.assertEqual("wait", runtime["current_action"]["action_type"])
        self.assertEqual("replan", runtime["reflection"]["next_action"])
        self.assertEqual("goal_preempted", runtime["task_state"]["latest_result"]["reason_code"])
        self.assertEqual("PREEMPTING", runtime["portfolio"]["hold_status"])
        self.assertEqual("replan_after_preemption", runtime["portfolio"]["resume_strategy"])

    def test_sync_goal_progress_inserts_resume_subgoal_after_preemption(self) -> None:
        repo = _FakeGoalRepo()
        runtime = build_preempted_goal_runtime(
            {
                "status": "CANCELLED",
                "current_phase": "wait",
                "current_action": {"action_type": "wait"},
                "task_state": {"latest_result": {"status": "CANCELLED"}},
                "policy": {"selected_action": "workflow_call"},
                "reflection": {"next_action": "workflow_call"},
            },
            goal_row={
                "goal_state": {
                    "portfolio": {
                        "hold_status": "PREEMPTING",
                        "preempted_task_id": "task-1",
                        "held_by_goal_id": "goal-urgent",
                        "hold_reason": "soft_preempted_by_urgent_goal",
                    }
                }
            },
            task_id="task-1",
        )
        synced = sync_goal_progress(
            repo=repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-preempt",
            goal={"normalized_goal": "resume deferred audit", "success_criteria": ["Collect facts", "Draft response"]},
            runtime_state=runtime,
            task_id="task-1",
        )

        dynamic_subgoals = [row for row in synced["goal_state"]["subgoals"] if row["kind"] == "dynamic"]
        self.assertTrue(any("Reassess plan after preemption" in row["title"] for row in dynamic_subgoals))
        self.assertTrue(any("Resume deferred work with updated priorities" in row["title"] for row in dynamic_subgoals))

    def test_policy_memory_bias_increases_agenda_priority(self) -> None:
        baseline_repo = _FakeGoalRepo()
        baseline = sync_goal_progress(
            repo=baseline_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-priority-1",
            goal={"normalized_goal": "prepare research brief", "risk_level": "medium"},
            runtime_state={
                "status": "RUNNING",
                "current_phase": "plan",
                "current_action": {"action_type": "workflow_call"},
                "policy": {"selected_action": "workflow_call", "policy_memory": {"action_bias": {"workflow_call": 0}}},
            },
        )
        biased_repo = _FakeGoalRepo()
        biased = sync_goal_progress(
            repo=biased_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-priority-2",
            goal={"normalized_goal": "prepare research brief", "risk_level": "medium"},
            runtime_state={
                "status": "RUNNING",
                "current_phase": "plan",
                "current_action": {"action_type": "workflow_call"},
                "policy": {"selected_action": "workflow_call", "policy_memory": {"action_bias": {"workflow_call": 12}}},
            },
        )

        self.assertGreater(
            float(biased["goal_state"]["agenda"]["priority_score"]),
            float(baseline["goal_state"]["agenda"]["priority_score"]),
        )
