import unittest
from unittest.mock import AsyncMock, patch

from app.config import settings
from app.services.goal_scheduler_service import (
    build_goal_continuation_request,
    dispatch_schedulable_goals,
    goal_needs_continuation,
)


class _FakeGoalRepo:
    def __init__(self, rows: list[dict], *, active_goal_count: int = 0) -> None:
        self.rows = list(rows)
        self.active_goal_count = active_goal_count
        self.attached: list[dict] = []
        self.claimed: list[str] = []
        self.held: list[dict] = []

    def list_schedulable_goals(self, *, cooldown_s: int, limit: int):
        del cooldown_s
        return list(self.rows[:limit])

    def count_goals_with_live_task(self):
        return self.active_goal_count

    def claim_goal_for_scheduler(self, *, tenant_id: str, goal_id: str, cooldown_s: int):
        del tenant_id, cooldown_s
        self.claimed.append(goal_id)
        for row in self.rows:
            if str(row.get("goal_id") or "") == goal_id:
                return dict(row)
        return None

    def list_live_goals(self, *, limit: int = 20):
        return list(self.rows[:limit])

    def get_goal(self, *, tenant_id: str, goal_id: str):
        del tenant_id
        for row in self.rows:
            if str(row.get("goal_id") or "") == goal_id:
                return dict(row)
        return None

    def update_goal_portfolio(self, *, tenant_id: str, goal_id: str, portfolio: dict):
        del tenant_id
        self.held.append({"goal_id": goal_id, "portfolio": dict(portfolio)})
        for row in self.rows:
            if str(row.get("goal_id") or "") == goal_id:
                state = dict(row.get("goal_state") or {})
                state["portfolio"] = dict(portfolio)
                row["goal_state"] = state

    def attach_task_to_goal(self, *, tenant_id: str, goal_id: str, task_id: str, goal_state: dict):
        self.attached.append(
            {
                "tenant_id": tenant_id,
                "goal_id": goal_id,
                "task_id": task_id,
                "goal_state": goal_state,
            }
        )


class GoalSchedulerServiceTests(unittest.IsolatedAsyncioTestCase):
    class _FakeTaskRepo:
        def __init__(self, runs_by_task_id: dict[str, dict] | None = None) -> None:
            self.runs_by_task_id = dict(runs_by_task_id or {})

        def get_latest_run_for_task(self, tenant_id: str, task_id: str):
            del tenant_id
            return self.runs_by_task_id.get(task_id)

    class _FakePolicyRepo:
        def __init__(self) -> None:
            self.rows: dict[str, dict] = {}
            self.active_id: str | None = None
            self.candidate_id: str | None = None
            self.canary_id: str | None = None

        def get_active_version(self, *, tenant_id: str):
            del tenant_id
            return self.rows.get(self.active_id or "")

        def get_candidate_version(self, *, tenant_id: str):
            del tenant_id
            return self.rows.get(self.candidate_id or self.canary_id or "")

        def create_policy_version(self, **kwargs):
            row = dict(kwargs)
            row["memory_payload"] = dict(kwargs["memory_payload"])
            row["comparison_payload"] = dict(kwargs["comparison_payload"])
            self.rows[row["version_id"]] = row
            if row["status"] == "ACTIVE":
                self.active_id = row["version_id"]
            if row["status"] == "CANDIDATE":
                self.candidate_id = row["version_id"]
            if row["status"] == "CANARY":
                self.canary_id = row["version_id"]
            return row

        def update_policy_version(self, *, tenant_id: str, version_id: str, memory_payload: dict, comparison_payload: dict):
            del tenant_id
            self.rows[version_id]["memory_payload"] = dict(memory_payload)
            self.rows[version_id]["comparison_payload"] = dict(comparison_payload)

        def get_policy_version(self, *, tenant_id: str, version_id: str):
            del tenant_id
            return self.rows.get(version_id)

        def mark_policy_version_status(self, *, tenant_id: str, version_id: str, status: str):
            del tenant_id
            self.rows[version_id]["status"] = status
            if self.candidate_id == version_id and status != "CANDIDATE":
                self.candidate_id = None
            if self.canary_id == version_id and status != "CANARY":
                self.canary_id = None
            if status == "CANDIDATE":
                self.candidate_id = version_id
            if status == "CANARY":
                self.canary_id = version_id

    async def test_dispatch_schedulable_goals_starts_continuation_task(self) -> None:
        goal_repo = _FakeGoalRepo(
            [
                {
                    "tenant_id": "default",
                    "goal_id": "goal-1",
                    "user_id": "user-1",
                    "conversation_id": "conv-1",
                    "continuation_count": 1,
                    "status": "ACTIVE",
                    "last_turn_id": "turn-1",
                    "goal_state": {
                        "goal": {"normalized_goal": "prepare quarterly report", "goal_id": "goal-1"},
                        "planner": {"task_type": "research_summary"},
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "task_state": {"current_phase": "wait"},
                        "policy": {"selected_action": "workflow_call"},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "agenda": {"priority_score": 0.72},
                    },
                }
            ]
        )
        with patch(
            "app.services.goal_scheduler_service.service_create_task",
            AsyncMock(return_value={"task_id": "task-goal", "run_id": "run-goal", "status": "QUEUED"}),
        ) as create_task:
            result = await dispatch_schedulable_goals(
                goal_repo=goal_repo,
                task_repo=object(),  # not used because service_create_task is mocked
                start_workflow=AsyncMock(),
                max_items=1,
            )

        self.assertEqual({"processed": 1, "scheduled": 1, "skipped": 0, "failed": 0, "preempted": 0}, result)
        self.assertEqual(1, create_task.await_count)
        req = create_task.await_args.kwargs["req"]
        self.assertEqual("goal-1", req.goal_id)
        self.assertEqual("goal_scheduler", req.origin)
        self.assertEqual("goal-1", req.input["runtime_state"]["goal"]["goal_id"])
        self.assertEqual("task-goal", goal_repo.attached[0]["task_id"])
        self.assertEqual("goal-1", goal_repo.claimed[0])

    def test_goal_needs_continuation_skips_waiting_for_user(self) -> None:
        self.assertFalse(
            goal_needs_continuation(
                {
                    "status": "WAITING",
                    "goal_state": {
                        "current_action": {"action_type": "ask_user"},
                        "reflection": {"next_action": "wait"},
                        "wake_condition": {"kind": "user_message"},
                    },
                }
            )
        )

    def test_goal_needs_continuation_skips_held_goal(self) -> None:
        self.assertFalse(
            goal_needs_continuation(
                {
                    "status": "ACTIVE",
                    "goal_state": {
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "portfolio": {"hold_status": "HELD", "hold_until": "2999-01-01T00:00:00+00:00"},
                    },
                }
            )
        )

    def test_build_goal_continuation_request_preserves_goal_identity(self) -> None:
        req = build_goal_continuation_request(
            {
                "goal_id": "goal-42",
                "user_id": "user-1",
                "conversation_id": "conv-42",
                "last_turn_id": "turn-42",
                "continuation_count": 2,
                "status": "ACTIVE",
                "goal_state": {
                    "goal": {"normalized_goal": "investigate flaky workflow", "goal_id": "goal-42"},
                    "planner": {"task_type": "research_summary"},
                    "current_action": {"action_type": "replan"},
                    "reflection": {"next_action": "workflow_call"},
                    "task_state": {"current_phase": "reflect"},
                    "active_subgoal": {"subgoal_id": "goal-42:sg:2", "sequence_no": 2, "status": "ACTIVE"},
                    "wake_condition": {"kind": "scheduler_cooldown", "resume_action": "workflow_call"},
                    "subgoals": [{"subgoal_id": "goal-42:sg:2", "sequence_no": 2, "status": "ACTIVE"}],
                },
            }
        )

        self.assertEqual("goal-42", req.goal_id)
        self.assertEqual("goal_scheduler", req.origin)
        self.assertEqual("workflow_call", req.input["runtime_state"]["current_action"]["action_type"])
        self.assertEqual(3, req.input["runtime_state"]["goal_ref"]["continuation_count"])
        self.assertEqual("goal-42:sg:2", req.input["runtime_state"]["goal_ref"]["active_subgoal_id"])
        self.assertEqual("goal-42:sg:2", req.input["runtime_state"]["scheduler"]["active_subgoal_id"])
        self.assertEqual(0.0, req.input["runtime_state"]["scheduler"]["priority_score"])

    def test_goal_needs_continuation_requires_scheduler_wake_condition(self) -> None:
        self.assertTrue(
            goal_needs_continuation(
                {
                    "status": "ACTIVE",
                    "goal_state": {
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "active_subgoal": {"status": "ACTIVE"},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                    },
                }
            )
        )
        self.assertFalse(
            goal_needs_continuation(
                {
                    "status": "ACTIVE",
                    "goal_state": {
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "active_subgoal": {"status": "WAITING"},
                        "wake_condition": {"kind": "approval"},
                    },
                }
            )
        )

    def test_goal_needs_continuation_skips_blocked_subgoal_dependencies(self) -> None:
        self.assertFalse(
            goal_needs_continuation(
                {
                    "status": "ACTIVE",
                    "goal_state": {
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "active_subgoal": {
                            "status": "PENDING",
                            "dependency_status": {"satisfied": False, "missing": ["goal-1:sg:1"]},
                        },
                        "wake_condition": {"kind": "scheduler_cooldown"},
                    },
                }
            )
        )

    def test_build_goal_continuation_request_carries_dependency_context(self) -> None:
        req = build_goal_continuation_request(
            {
                "goal_id": "goal-88",
                "user_id": "user-1",
                "conversation_id": "conv-88",
                "last_turn_id": "turn-88",
                "continuation_count": 0,
                "status": "ACTIVE",
                "goal_state": {
                    "goal": {"normalized_goal": "ship release", "goal_id": "goal-88"},
                    "planner": {"task_type": "research_summary"},
                    "current_action": {"action_type": "workflow_call"},
                    "reflection": {"next_action": "workflow_call"},
                    "task_state": {"current_phase": "plan"},
                    "active_subgoal": {
                        "subgoal_id": "goal-88:sg:2",
                        "sequence_no": 2,
                        "title": "Run rollout",
                        "status": "PENDING",
                        "dependency_status": {"satisfied": True, "missing": []},
                    },
                    "wake_condition": {"kind": "scheduler_cooldown", "resume_action": "workflow_call"},
                    "ready_subgoals": [{"subgoal_id": "goal-88:sg:2"}],
                    "blocked_subgoals": [{"subgoal_id": "goal-88:sg:3"}],
                },
            }
        )

        self.assertEqual("Run rollout", req.input["runtime_state"]["current_action"]["target"])
        self.assertEqual([{"subgoal_id": "goal-88:sg:2"}], req.input["ready_subgoals"])
        self.assertEqual([{"subgoal_id": "goal-88:sg:3"}], req.input["blocked_subgoals"])

    def test_goal_needs_continuation_skips_task_completion_waits(self) -> None:
        self.assertFalse(
            goal_needs_continuation(
                {
                    "status": "WAITING",
                    "goal_state": {
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "active_subgoal": {
                            "subgoal_id": "goal-9:sg:2",
                            "status": "WAITING",
                            "dependency_status": {"satisfied": True, "missing": []},
                        },
                        "wake_condition": {"kind": "task_completion", "event_key": "task-1"},
                        "wake_graph": {"resume_candidates": ["goal-9:sg:2"]},
                    },
                }
            )
        )

    def test_goal_needs_continuation_skips_pending_required_subscriptions(self) -> None:
        self.assertFalse(
            goal_needs_continuation(
                {
                    "status": "WAITING",
                    "goal_state": {
                        "current_action": {"action_type": "wait"},
                        "reflection": {"next_action": "workflow_call"},
                        "active_subgoal": {
                            "subgoal_id": "goal-10:sg:2",
                            "status": "WAITING",
                            "dependency_status": {"satisfied": True, "missing": []},
                        },
                        "wake_condition": {"kind": "composite", "event_key": "goal-10"},
                        "event_subscriptions": [
                            {"kind": "approval", "event_key": "approval-1", "required": True, "status": "satisfied"},
                            {"kind": "user_message", "event_key": "conv-10", "required": True, "status": "pending"},
                        ],
                    },
                }
            )
        )

    def test_goal_needs_continuation_allows_subscription_timeout_recovery(self) -> None:
        self.assertTrue(
            goal_needs_continuation(
                {
                    "status": "WAITING",
                    "goal_state": {
                        "current_action": {"action_type": "wait"},
                        "reflection": {"next_action": "workflow_call"},
                        "active_subgoal": {
                            "subgoal_id": "goal-11:sg:2",
                            "status": "WAITING",
                            "dependency_status": {"satisfied": True, "missing": []},
                        },
                        "wake_condition": {"kind": "scheduler_cooldown", "status": "subscription_timeout"},
                        "event_timeouts": {"expired_required_count": 1},
                        "event_subscriptions": [
                            {"kind": "external_signal", "event_key": "vendor-callback", "required": True, "status": "expired"},
                        ],
                    },
                }
            )
        )

    def test_build_goal_continuation_request_carries_wake_graph(self) -> None:
        req = build_goal_continuation_request(
            {
                "goal_id": "goal-99",
                "user_id": "user-1",
                "conversation_id": "conv-99",
                "last_turn_id": "turn-99",
                "continuation_count": 4,
                "status": "ACTIVE",
                "goal_state": {
                    "goal": {"normalized_goal": "repair rollout", "goal_id": "goal-99"},
                    "planner": {"task_type": "research_summary"},
                    "current_action": {"action_type": "workflow_call"},
                    "reflection": {"next_action": "workflow_call"},
                    "task_state": {"current_phase": "plan"},
                    "active_subgoal": {
                        "subgoal_id": "goal-99:dyn:repair:adapter-http-5xx",
                        "sequence_no": 4,
                        "title": "Recover and replan: adapter_http_5xx",
                        "status": "ACTIVE",
                        "dependency_status": {"satisfied": True, "missing": []},
                    },
                    "wake_condition": {"kind": "scheduler_cooldown", "resume_action": "workflow_call", "event_key": "goal-99"},
                    "wake_graph": {
                        "goal_id": "goal-99",
                        "active_subgoal_id": "goal-99:dyn:repair:adapter-http-5xx",
                        "resume_candidates": ["goal-99:dyn:repair:adapter-http-5xx"],
                    },
                },
            }
        )

        self.assertEqual("goal-99", req.input["wake_graph"]["goal_id"])
        self.assertEqual(
            "goal-99:dyn:repair:adapter-http-5xx",
            req.input["runtime_state"]["wake_graph"]["active_subgoal_id"],
        )

    def test_goal_needs_continuation_skips_zero_priority_agenda(self) -> None:
        self.assertFalse(
            goal_needs_continuation(
                {
                    "status": "ACTIVE",
                    "goal_state": {
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "active_subgoal": {"subgoal_id": "goal-3:sg:1", "status": "ACTIVE", "dependency_status": {"satisfied": True}},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "wake_graph": {"resume_candidates": ["goal-3:sg:1"]},
                        "agenda": {"priority_score": 0.0},
                    },
                }
            )
        )

    def test_build_goal_continuation_request_carries_agenda_priority(self) -> None:
        req = build_goal_continuation_request(
            {
                "goal_id": "goal-101",
                "user_id": "user-1",
                "conversation_id": "conv-101",
                "last_turn_id": "turn-101",
                "continuation_count": 1,
                "status": "ACTIVE",
                "goal_state": {
                    "goal": {"normalized_goal": "stabilize rollout", "goal_id": "goal-101"},
                    "planner": {"task_type": "research_summary"},
                    "current_action": {"action_type": "workflow_call"},
                    "reflection": {"next_action": "workflow_call"},
                    "task_state": {"current_phase": "plan"},
                    "active_subgoal": {
                        "subgoal_id": "goal-101:dyn:repair:1",
                        "sequence_no": 3,
                        "title": "Recover and replan: adapter_http_5xx",
                        "status": "ACTIVE",
                        "dependency_status": {"satisfied": True, "missing": []},
                    },
                    "wake_condition": {"kind": "scheduler_cooldown", "resume_action": "workflow_call"},
                    "agenda": {"priority_score": 0.91, "priority_bucket": "urgent"},
                },
            }
        )

        self.assertEqual(0.91, req.input["agenda"]["priority_score"])
        self.assertEqual(0.91, req.input["runtime_state"]["scheduler"]["priority_score"])

    def test_build_goal_continuation_request_replans_after_preemption(self) -> None:
        req = build_goal_continuation_request(
            {
                "goal_id": "goal-resume",
                "user_id": "user-1",
                "conversation_id": "conv-resume",
                "last_turn_id": "turn-resume",
                "continuation_count": 3,
                "status": "WAITING",
                "goal_state": {
                    "goal": {"normalized_goal": "resume deferred audit", "goal_id": "goal-resume"},
                    "planner": {"task_type": "research_summary"},
                    "current_action": {"action_type": "wait"},
                    "reflection": {"next_action": "workflow_call"},
                    "task_state": {"current_phase": "wait", "latest_result": {"status": "CANCELLED"}},
                    "active_subgoal": {
                        "subgoal_id": "goal-resume:dyn:resume:1",
                        "sequence_no": 3,
                        "title": "Reassess plan after preemption by goal-urgent",
                        "status": "WAITING",
                        "dependency_status": {"satisfied": True, "missing": []},
                    },
                    "wake_condition": {"kind": "scheduler_cooldown", "resume_action": "workflow_call"},
                    "portfolio": {
                        "resume_strategy": "replan_after_preemption",
                        "last_held_by_goal_id": "goal-urgent",
                        "last_hold_reason": "soft_preempted_by_urgent_goal",
                    },
                },
            }
        )

        self.assertEqual("replan", req.input["runtime_state"]["current_action"]["action_type"])
        self.assertEqual("replan", req.input["runtime_state"]["current_phase"])
        self.assertTrue(req.input["runtime_state"]["scheduler"]["preemption_recovery"])
        self.assertEqual("goal_preempted", req.input["runtime_state"]["task_state"]["latest_result"]["reason_code"])

    def test_build_goal_continuation_request_replans_after_subscription_timeout(self) -> None:
        req = build_goal_continuation_request(
            {
                "goal_id": "goal-timeout",
                "user_id": "user-1",
                "conversation_id": "conv-timeout",
                "last_turn_id": "turn-timeout",
                "continuation_count": 1,
                "status": "WAITING",
                "goal_state": {
                    "goal": {"normalized_goal": "wait for vendor callback", "goal_id": "goal-timeout"},
                    "planner": {"task_type": "research_summary"},
                    "current_action": {"action_type": "wait"},
                    "reflection": {"next_action": "workflow_call"},
                    "task_state": {"current_phase": "wait", "latest_result": {"status": "WAITING_HUMAN"}},
                    "active_subgoal": {
                        "subgoal_id": "goal-timeout:sg:1",
                        "sequence_no": 1,
                        "title": "Wait for vendor callback",
                        "status": "WAITING",
                        "dependency_status": {"satisfied": True, "missing": []},
                    },
                    "wake_condition": {"kind": "scheduler_cooldown", "status": "subscription_timeout", "resume_action": "workflow_call"},
                    "event_timeouts": {"expired_required_count": 1},
                    "event_subscriptions": [
                        {
                            "subscription_id": "goal-timeout:sub:1",
                            "kind": "external_signal",
                            "event_key": "vendor-callback",
                            "required": True,
                            "status": "expired",
                        }
                    ],
                },
            }
        )

        self.assertEqual("replan", req.input["runtime_state"]["current_action"]["action_type"])
        self.assertEqual("replan", req.input["runtime_state"]["current_phase"])
        self.assertTrue(req.input["runtime_state"]["scheduler"]["subscription_timeout_recovery"])
        self.assertEqual("subscription_timeout", req.input["runtime_state"]["task_state"]["latest_result"]["reason_code"])

    async def test_dispatch_schedulable_goals_prefers_high_priority_goal(self) -> None:
        goal_repo = _FakeGoalRepo(
            [
                {
                    "tenant_id": "default",
                    "goal_id": "goal-low",
                    "user_id": "user-1",
                    "conversation_id": "conv-low",
                    "continuation_count": 1,
                    "status": "ACTIVE",
                    "last_turn_id": "turn-low",
                    "goal_state": {
                        "goal": {"normalized_goal": "background follow-up", "goal_id": "goal-low"},
                        "planner": {"task_type": "research_summary"},
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "task_state": {"current_phase": "plan"},
                        "policy": {"selected_action": "workflow_call"},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "agenda": {"priority_score": 0.31},
                    },
                },
                {
                    "tenant_id": "default",
                    "goal_id": "goal-high",
                    "user_id": "user-1",
                    "conversation_id": "conv-high",
                    "continuation_count": 2,
                    "status": "ACTIVE",
                    "last_turn_id": "turn-high",
                    "goal_state": {
                        "goal": {"normalized_goal": "stabilize urgent outage", "goal_id": "goal-high"},
                        "planner": {"task_type": "research_summary"},
                        "current_action": {"action_type": "replan"},
                        "reflection": {"next_action": "workflow_call"},
                        "task_state": {"current_phase": "reflect"},
                        "policy": {"selected_action": "replan"},
                        "active_subgoal": {"kind": "dynamic", "status": "ACTIVE", "dependency_status": {"satisfied": True}},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "agenda": {"priority_score": 0.84, "active_subgoal_kind": "dynamic"},
                    },
                },
            ]
        )
        with patch(
            "app.services.goal_scheduler_service.service_create_task",
            AsyncMock(return_value={"task_id": "task-high", "run_id": "run-high", "status": "QUEUED"}),
        ) as create_task:
            result = await dispatch_schedulable_goals(
                goal_repo=goal_repo,
                task_repo=object(),
                start_workflow=AsyncMock(),
                max_items=1,
            )

        self.assertEqual({"processed": 1, "scheduled": 1, "skipped": 1, "failed": 0, "preempted": 0}, result)
        req = create_task.await_args.kwargs["req"]
        self.assertEqual("goal-high", req.goal_id)
        self.assertGreater(req.budget, 1.0)
        self.assertEqual("goal-high", goal_repo.claimed[0])

    async def test_dispatch_schedulable_goals_soft_preempts_urgent_goal_when_budget_full(self) -> None:
        goal_repo = _FakeGoalRepo(
            [
                {
                    "tenant_id": "default",
                    "goal_id": "goal-held",
                    "user_id": "user-1",
                    "conversation_id": "conv-held",
                    "continuation_count": 0,
                    "status": "ACTIVE",
                    "last_turn_id": "turn-held",
                    "current_task_id": "task-live",
                    "goal_state": {
                        "goal": {"normalized_goal": "routine reconciliation", "goal_id": "goal-held"},
                        "planner": {"task_type": "research_summary"},
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "task_state": {"current_phase": "plan"},
                        "policy": {"selected_action": "workflow_call"},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "agenda": {"priority_score": 0.18},
                    },
                },
                {
                    "tenant_id": "default",
                    "goal_id": "goal-urgent",
                    "user_id": "user-1",
                    "conversation_id": "conv-urgent",
                    "continuation_count": 4,
                    "status": "ACTIVE",
                    "last_turn_id": "turn-urgent",
                    "goal_state": {
                        "goal": {"normalized_goal": "recover critical service", "goal_id": "goal-urgent"},
                        "planner": {"task_type": "research_summary"},
                        "current_action": {"action_type": "replan"},
                        "reflection": {"next_action": "workflow_call"},
                        "task_state": {"current_phase": "reflect"},
                        "policy": {
                            "selected_action": "replan",
                            "policy_memory": {"portfolio_bias": {"dynamic_subgoal_boost": 3, "stalled_goal_boost": 4}},
                        },
                        "active_subgoal": {"kind": "dynamic", "status": "ACTIVE", "dependency_status": {"satisfied": True}},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "agenda": {"priority_score": 0.89, "active_subgoal_kind": "dynamic"},
                    },
                }
            ],
            active_goal_count=4,
        )
        task_repo = self._FakeTaskRepo({"task-live": {"workflow_id": "wf-held"}})
        policy_repo = self._FakePolicyRepo()
        with patch(
            "app.services.goal_scheduler_service.service_create_task",
            AsyncMock(return_value={"task_id": "task-urgent", "run_id": "run-urgent", "status": "QUEUED"}),
        ) as create_task, patch(
            "app.services.goal_scheduler_service.record_portfolio_feedback"
        ) as record_feedback:
            cancel = AsyncMock()
            result = await dispatch_schedulable_goals(
                goal_repo=goal_repo,
                task_repo=task_repo,
                start_workflow=AsyncMock(),
                cancel_workflow=cancel,
                policy_repo=policy_repo,
                max_items=1,
            )

        self.assertEqual({"processed": 1, "scheduled": 1, "skipped": 1, "failed": 0, "preempted": 1}, result)
        req = create_task.await_args.kwargs["req"]
        self.assertTrue(req.input["runtime_state"]["scheduler"]["soft_preempt"])
        self.assertEqual("soft_preempt", req.input["portfolio"]["dispatch_decision"])
        cancel.assert_awaited_once_with("wf-held")
        self.assertEqual("goal-held", goal_repo.held[0]["goal_id"])
        self.assertEqual("PREEMPTING", goal_repo.held[0]["portfolio"]["hold_status"])
        record_feedback.assert_called()

    async def test_dispatch_schedulable_goals_records_subscription_timeout_learning(self) -> None:
        goal_repo = _FakeGoalRepo(
            [
                {
                    "tenant_id": "default",
                    "goal_id": "goal-timeout",
                    "user_id": "user-1",
                    "conversation_id": "conv-timeout",
                    "continuation_count": 1,
                    "status": "WAITING",
                    "last_turn_id": "turn-timeout",
                    "goal_state": {
                        "goal": {"normalized_goal": "wait for vendor callback", "goal_id": "goal-timeout"},
                        "planner": {"task_type": "research_summary"},
                        "current_action": {"action_type": "wait"},
                        "reflection": {"next_action": "workflow_call"},
                        "task_state": {"current_phase": "wait", "latest_result": {"status": "WAITING_HUMAN"}},
                        "policy": {"selected_action": "workflow_call"},
                        "active_subgoal": {
                            "subgoal_id": "goal-timeout:sg:1",
                            "status": "WAITING",
                            "dependency_status": {"satisfied": True},
                        },
                        "wake_condition": {"kind": "scheduler_cooldown", "status": "subscription_timeout", "resume_action": "workflow_call"},
                        "wake_graph": {"resume_candidates": ["goal-timeout:sg:1"]},
                        "agenda": {"priority_score": 0.76},
                        "event_timeouts": {"expired_required_count": 1},
                        "event_subscriptions": [
                            {"subscription_id": "goal-timeout:sub:1", "kind": "external_signal", "event_key": "vendor-callback", "required": True, "status": "expired"},
                        ],
                    },
                }
            ]
        )
        with patch(
            "app.services.goal_scheduler_service.service_create_task",
            AsyncMock(return_value={"task_id": "task-timeout", "run_id": "run-timeout", "status": "QUEUED"}),
        ), patch("app.services.goal_scheduler_service.record_portfolio_feedback") as record_feedback:
            result = await dispatch_schedulable_goals(
                goal_repo=goal_repo,
                task_repo=self._FakeTaskRepo(),
                start_workflow=AsyncMock(),
                policy_repo=self._FakePolicyRepo(),
                max_items=1,
            )

        self.assertEqual({"processed": 1, "scheduled": 1, "skipped": 0, "failed": 0, "preempted": 0}, result)
        self.assertTrue(
            any(
                kwargs.get("feedback", {}).get("event_kind") == "subscription_timeout"
                for _, kwargs in record_feedback.call_args_list
            )
        )

    async def test_dispatch_schedulable_goals_records_starvation_for_deferred_focus_goal(self) -> None:
        goal_repo = _FakeGoalRepo(
            [
                {
                    "tenant_id": "default",
                    "goal_id": "goal-top",
                    "user_id": "user-1",
                    "conversation_id": "conv-top",
                    "continuation_count": 0,
                    "status": "ACTIVE",
                    "last_turn_id": "turn-top",
                    "updated_at": "2026-03-15T09:00:00+00:00",
                    "goal_state": {
                        "goal": {"normalized_goal": "top priority incident", "goal_id": "goal-top"},
                        "planner": {"task_type": "research_summary"},
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "task_state": {"current_phase": "plan"},
                        "policy": {"selected_action": "workflow_call"},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "agenda": {"priority_score": 0.95},
                    },
                },
                {
                    "tenant_id": "default",
                    "goal_id": "goal-starved",
                    "user_id": "user-1",
                    "conversation_id": "conv-starved",
                    "continuation_count": 2,
                    "status": "ACTIVE",
                    "last_turn_id": "turn-starved",
                    "updated_at": "2026-03-15T08:00:00+00:00",
                    "goal_state": {
                        "goal": {"normalized_goal": "important deferred follow-up", "goal_id": "goal-starved"},
                        "planner": {"task_type": "research_summary"},
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "task_state": {"current_phase": "plan"},
                        "policy": {"selected_action": "workflow_call"},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "agenda": {"priority_score": 0.78},
                    },
                },
            ]
        )
        with (
            patch.object(settings, "goal_scheduler_starvation_score_threshold", 0.7),
            patch.object(settings, "goal_scheduler_starvation_min_age_min", 0.0),
            patch(
                "app.services.goal_scheduler_service.service_create_task",
                AsyncMock(return_value={"task_id": "task-top", "run_id": "run-top", "status": "QUEUED"}),
            ),
            patch("app.services.goal_scheduler_service.record_portfolio_feedback") as record_feedback,
        ):
            result = await dispatch_schedulable_goals(
                goal_repo=goal_repo,
                task_repo=self._FakeTaskRepo(),
                start_workflow=AsyncMock(),
                policy_repo=self._FakePolicyRepo(),
                max_items=1,
            )

        self.assertEqual({"processed": 1, "scheduled": 1, "skipped": 1, "failed": 0, "preempted": 0}, result)
        self.assertTrue(
            any(
                kwargs.get("feedback", {}).get("event_kind") == "goal_starved"
                and kwargs.get("feedback", {}).get("goal_id") == "goal-starved"
                for _, kwargs in record_feedback.call_args_list
            )
        )

    async def test_dispatch_schedulable_goals_records_shadow_portfolio_probe_for_canary(self) -> None:
        goal_repo = _FakeGoalRepo(
            [
                {
                    "tenant_id": "default",
                    "goal_id": "goal-routine",
                    "user_id": "user-1",
                    "conversation_id": "conv-routine",
                    "continuation_count": 0,
                    "status": "ACTIVE",
                    "last_turn_id": "turn-routine",
                    "goal_state": {
                        "goal": {"normalized_goal": "routine reconciliation", "goal_id": "goal-routine"},
                        "planner": {"task_type": "research_summary"},
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "task_state": {"current_phase": "plan"},
                        "policy": {"selected_action": "workflow_call"},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "agenda": {"priority_score": 0.83},
                    },
                },
                {
                    "tenant_id": "default",
                    "goal_id": "goal-dynamic",
                    "user_id": "user-1",
                    "conversation_id": "conv-dynamic",
                    "continuation_count": 1,
                    "status": "ACTIVE",
                    "last_turn_id": "turn-dynamic",
                    "goal_state": {
                        "goal": {"normalized_goal": "recover critical service", "goal_id": "goal-dynamic"},
                        "planner": {"task_type": "research_summary"},
                        "current_action": {"action_type": "replan"},
                        "reflection": {"next_action": "workflow_call"},
                        "task_state": {"current_phase": "reflect"},
                        "policy": {"selected_action": "replan"},
                        "active_subgoal": {"kind": "dynamic", "status": "ACTIVE", "dependency_status": {"satisfied": True}},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "agenda": {"priority_score": 0.66, "active_subgoal_kind": "dynamic"},
                    },
                },
            ]
        )
        policy_repo = self._FakePolicyRepo()
        policy_repo.create_policy_version(
            tenant_id="default",
            version_id="policy-active",
            version_tag="active",
            status="ACTIVE",
            base_version_id=None,
            source="bootstrap",
            memory_payload={"eval_summary": {"success_rate": 0.95}},
            comparison_payload={},
            created_by="user-1",
        )
        policy_repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id="policy-active",
            source="eval_feedback",
            memory_payload={
                "eval_summary": {"success_rate": 0.97},
                "portfolio_bias": {"dynamic_subgoal_boost": 20, "replan_goal_boost": 20},
            },
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_shadow_enabled", True),
            patch.object(settings, "policy_shadow_auto_rollback_enabled", False),
            patch.object(settings, "policy_shadow_min_portfolio_probe_count", 2),
            patch(
                "app.services.goal_scheduler_service.service_create_task",
                AsyncMock(return_value={"task_id": "task-routine", "run_id": "run-routine", "status": "QUEUED"}),
            ) as create_task,
        ):
            result = await dispatch_schedulable_goals(
                goal_repo=goal_repo,
                task_repo=object(),
                start_workflow=AsyncMock(),
                policy_repo=policy_repo,
                max_items=1,
            )

        self.assertEqual({"processed": 1, "scheduled": 1, "skipped": 1, "failed": 0, "preempted": 0}, result)
        comparison = policy_repo.rows["policy-canary"]["comparison_payload"]
        self.assertEqual(1, comparison["shadow_portfolio_counts"]["total"])
        self.assertEqual(1, comparison["shadow_portfolio_counts"]["selected_divergence"])
        self.assertEqual(["goal-routine"], comparison["shadow_portfolio_last_probe"]["live_selected_goal_ids"])
        self.assertEqual(["goal-dynamic"], comparison["shadow_portfolio_last_probe"]["shadow_selected_goal_ids"])
        req = create_task.await_args.kwargs["req"]
        self.assertEqual("policy-canary", req.input["portfolio"]["shadow_portfolio"]["version_id"])
        self.assertEqual(["goal-dynamic"], req.input["portfolio"]["shadow_portfolio"]["shadow_selected_goal_ids"])

    async def test_dispatch_schedulable_goals_shadow_probe_uses_candidate_external_source_learning(self) -> None:
        goal_repo = _FakeGoalRepo(
            [
                {
                    "tenant_id": "default",
                    "goal_id": "goal-external",
                    "user_id": "user-1",
                    "conversation_id": "conv-external",
                    "continuation_count": 0,
                    "status": "ACTIVE",
                    "last_turn_id": "turn-external",
                    "goal_state": {
                        "goal": {"normalized_goal": "wait on vendor callback", "goal_id": "goal-external"},
                        "planner": {"task_type": "research_summary"},
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "task_state": {"current_phase": "plan"},
                        "policy": {"selected_action": "workflow_call"},
                        "wake_condition": {"kind": "external_signal", "source": "vendor_webhook"},
                        "wake_graph": {"waiting_events": [{"kind": "external_signal"}]},
                        "agenda": {"priority_score": 0.85},
                    },
                },
                {
                    "tenant_id": "default",
                    "goal_id": "goal-local",
                    "user_id": "user-1",
                    "conversation_id": "conv-local",
                    "continuation_count": 0,
                    "status": "ACTIVE",
                    "last_turn_id": "turn-local",
                    "goal_state": {
                        "goal": {"normalized_goal": "continue local workflow", "goal_id": "goal-local"},
                        "planner": {"task_type": "research_summary"},
                        "current_action": {"action_type": "workflow_call"},
                        "reflection": {"next_action": "workflow_call"},
                        "task_state": {"current_phase": "plan"},
                        "policy": {"selected_action": "workflow_call"},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "agenda": {"priority_score": 0.81},
                    },
                },
            ]
        )
        policy_repo = self._FakePolicyRepo()
        policy_repo.create_policy_version(
            tenant_id="default",
            version_id="policy-active",
            version_tag="active",
            status="ACTIVE",
            base_version_id=None,
            source="bootstrap",
            memory_payload={"eval_summary": {"success_rate": 0.95}},
            comparison_payload={},
            created_by="user-1",
        )
        policy_repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id="policy-active",
            source="eval_feedback",
            memory_payload={
                "eval_summary": {"success_rate": 0.97},
                "external_source_reliability": {
                    "vendor_webhook": {"score": -0.95, "confidence": 0.95},
                },
            },
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_shadow_enabled", True),
            patch.object(settings, "policy_shadow_auto_rollback_enabled", False),
            patch.object(settings, "policy_shadow_min_portfolio_probe_count", 2),
        ):
            result = await dispatch_schedulable_goals(
                goal_repo=goal_repo,
                task_repo=object(),
                start_workflow=AsyncMock(),
                policy_repo=policy_repo,
                max_items=1,
            )

        self.assertEqual({"processed": 1, "scheduled": 0, "skipped": 2, "failed": 0, "preempted": 0}, result)
        comparison = policy_repo.rows["policy-canary"]["comparison_payload"]
        self.assertEqual(["goal-external"], comparison["shadow_portfolio_last_probe"]["live_selected_goal_ids"])
        self.assertEqual(["goal-local"], comparison["shadow_portfolio_last_probe"]["shadow_selected_goal_ids"])
        self.assertEqual(["vendor_webhook"], comparison["shadow_portfolio_last_probe"]["live_external_wait_sources"])
        self.assertEqual([], comparison["shadow_portfolio_last_probe"]["shadow_external_wait_sources"])
        self.assertEqual(0.0, comparison["shadow_portfolio_summary"]["shadow_portfolio_external_wait_agreement_rate"])
