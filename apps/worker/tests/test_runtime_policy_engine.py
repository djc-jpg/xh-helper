import unittest

import activities
import workflows
from runtime_backbone import (
    apply_runtime_event,
    choose_next_action,
    derive_experience_profile,
    derive_runtime_followup,
    recommend_goal_holds,
    route_for_action_type,
    runtime_requires_approval,
    score_goal_portfolio_entry,
    select_goal_portfolio_slice,
    select_next_runtime_step,
)


class RuntimePolicyEngineTests(unittest.TestCase):
    def test_choose_next_action_and_route_share_policy_contract(self) -> None:
        action, policy = choose_next_action(
            goal={
                "normalized_goal": "prepare a research summary report",
                "unknowns": [],
                "risk_level": "medium",
            },
            planner={"action": "start_workflow"},
            task_state={"available_actions": ["respond", "workflow_call"]},
            retrieval_hits=[],
            tool_candidates=[],
            confirmed=False,
            episodes=[],
            has_retrieval_observation=False,
            latest_result=None,
            requested_mode="auto",
        )

        self.assertEqual("workflow_call", action["action_type"])
        self.assertEqual("workflow_call", policy["selected_action"])
        self.assertEqual("workflow_task", route_for_action_type(action["action_type"]))
        self.assertTrue(action["expected_result"])
        self.assertEqual("respond", action["fallback"])
        self.assertIn("runtime_state_advanced", action["success_conditions"])
        self.assertIn("workflow_failed_final", action["stop_conditions"])

    def test_choose_next_action_respects_explicit_durable_runtime_request(self) -> None:
        action, policy = choose_next_action(
            goal={
                "normalized_goal": "prepare a grounded summary and continue through the durable runtime if needed",
                "unknowns": [],
                "risk_level": "low",
            },
            planner={"action": "answer_only"},
            task_state={"available_actions": ["respond", "workflow_call"]},
            retrieval_hits=[],
            tool_candidates=[],
            confirmed=False,
            episodes=[],
            has_retrieval_observation=True,
            latest_result=None,
            requested_mode="auto",
        )

        self.assertEqual("workflow_call", action["action_type"])
        self.assertEqual("workflow_call", policy["selected_action"])
        self.assertIn("durable workflow path", action["rationale"])

    def test_choose_next_action_keeps_explanatory_question_in_answer_path(self) -> None:
        action, policy = choose_next_action(
            goal={
                "normalized_goal": "How does the workflow runtime work in this repo?",
                "unknowns": [],
                "risk_level": "low",
            },
            planner={
                "action": "use_retrieval",
                "intent": "general_qna",
                "policy_signals": {
                    "action_signal": "retrieve",
                    "action_affinities": {"retrieve": 0.8, "workflow_call": 0.2},
                },
            },
            task_state={"available_actions": ["retrieve", "respond", "workflow_call"]},
            retrieval_hits=[{"title": "runtime", "snippet": "..."}],
            tool_candidates=[],
            confirmed=False,
            episodes=[
                {
                    "episode_id": "ep-workflow",
                    "chosen_strategy": "workflow_call",
                    "outcome_status": "SUCCEEDED",
                }
            ],
            has_retrieval_observation=True,
            latest_result=None,
            requested_mode="auto",
        )

        self.assertEqual("respond", action["action_type"])
        self.assertEqual("respond", policy["selected_action"])
        self.assertIn("explanatory question", action["rationale"])

    def test_choose_next_action_keeps_chinese_explanatory_question_in_answer_path(self) -> None:
        action, policy = choose_next_action(
            goal={
                "normalized_goal": "这个 workflow runtime 是怎么工作的？",
                "unknowns": [],
                "risk_level": "low",
            },
            planner={
                "action": "use_retrieval",
                "intent": "general_qna",
                "policy_signals": {
                    "action_signal": "retrieve",
                    "action_affinities": {"retrieve": 0.8, "workflow_call": 0.2},
                },
            },
            task_state={"available_actions": ["retrieve", "respond", "workflow_call"]},
            retrieval_hits=[{"title": "runtime", "snippet": "..."}],
            tool_candidates=[],
            confirmed=False,
            episodes=[],
            has_retrieval_observation=True,
            latest_result=None,
            requested_mode="auto",
        )

        self.assertEqual("respond", action["action_type"])
        self.assertEqual("respond", policy["selected_action"])

    def test_runtime_requires_approval_for_pending_tool_plan(self) -> None:
        requires_hitl = runtime_requires_approval(
            task_type="research_summary",
            current_action={"action_type": "tool_call", "requires_approval": False},
            policy={"approval_triggered": False},
            pending_tool_plans=[{"tool_id": "internal_rest_api"}],
        )

        self.assertTrue(requires_hitl)

    def test_workflow_runtime_patch_uses_shared_reducer(self) -> None:
        runtime = workflows._runtime_patch(
            {"task_state": {"pending_approvals": []}},
            status="WAITING_APPROVAL",
            current_phase="approval_wait",
            latest_result={"status": "waiting_for_approval"},
            pending_approvals=["approval-1"],
            reflection={"summary": "Waiting for approval.", "requires_replan": False},
        )

        self.assertEqual("approval_wait", runtime["current_phase"])
        self.assertEqual("workflow.approval_wait", runtime["runtime_event"]["type"])
        self.assertEqual(["approval-1"], runtime["task_state"]["pending_approvals"])
        self.assertEqual("wait", runtime["current_action"]["action_type"])
        self.assertTrue(runtime["policy"]["approval_triggered"])

    def test_graph_runtime_patch_uses_shared_reducer(self) -> None:
        runtime = activities._runtime_from_graph_result(
            {"agent_runtime": {"task_state": {"current_phase": "plan"}}},
            status="SUCCEEDED",
            current_phase="reflect",
            latest_result={"status": "review_ready"},
            reflection={"summary": "Reviewed output.", "requires_replan": False},
            final_output={"message": "done"},
        )

        self.assertEqual("reflect", runtime["current_phase"])
        self.assertEqual("graph.reflect", runtime["runtime_event"]["type"])
        self.assertEqual("review_ready", runtime["task_state"]["latest_result"]["status"])

    def test_derive_runtime_followup_replans_retryable_failure(self) -> None:
        action, policy, reflection = derive_runtime_followup(
            {"task_state": {"available_actions": ["workflow_call", "respond"]}, "policy": {"fallback_action": "respond"}},
            event_type="workflow.error",
            status="FAILED_RETRYABLE",
            current_phase="reflect",
            latest_result={"status": "FAILED_RETRYABLE", "failure_type": "UPSTREAM_TIMEOUT", "reason": "timeout"},
            summary="timeout",
        )

        self.assertEqual("replan", action["action_type"])
        self.assertEqual("replan", policy["selected_action"])
        self.assertTrue(reflection["requires_replan"])
        self.assertEqual("replan", reflection["next_action"])

    def test_apply_runtime_event_preserves_existing_episodes(self) -> None:
        runtime = apply_runtime_event(
            {"episodes": [{"episode_id": "ep-1"}], "task_state": {}},
            event_type="workflow.plan",
            status="PLANNING",
            current_phase="plan",
            latest_result={"status": "PLANNING"},
        )

        self.assertEqual("ep-1", runtime["episodes"][0]["episode_id"])
        self.assertEqual("workflow_call", runtime["current_action"]["action_type"])

    def test_select_next_runtime_step_packages_initial_decision(self) -> None:
        selection = select_next_runtime_step(
            goal={"normalized_goal": "help with it", "unknowns": ["ambiguous_user_reference"], "risk_level": "low"},
            planner={"action": "answer_only", "intent": "general_qna"},
            task_state={"available_actions": ["ask_user", "retrieve", "respond"]},
            retrieval_hits=[],
            tool_candidates=[],
            confirmed=False,
            episodes=[],
            has_retrieval_observation=False,
            requested_mode="auto",
            selected_tool=None,
        )

        self.assertEqual("ask_user", selection["current_action"]["action_type"])
        self.assertEqual("direct_answer", selection["route"])
        self.assertEqual("wait", selection["reflection"]["next_action"])
        self.assertTrue(selection["decision"]["candidate_actions"])
        self.assertIn("respond", selection["decision"]["why_not"])

    def test_experience_profile_detects_tool_retry_pattern(self) -> None:
        profile = derive_experience_profile(
            [
                {
                    "chosen_strategy": "tool_call",
                    "outcome_status": "FAILED_RETRYABLE",
                    "tool_names": ["web_search"],
                    "useful_lessons": ["Retryable failures should escalate into a different action type."],
                }
            ],
            selected_tool="web_search",
        )

        self.assertEqual(1, profile["tool_retry_failures"])
        self.assertEqual("respond", profile["preferred_action"])

    def test_choose_next_action_biases_to_workflow_after_tool_retry_failures(self) -> None:
        action, policy = choose_next_action(
            goal={"normalized_goal": "search docs", "unknowns": [], "risk_level": "low"},
            planner={"action": "use_tool", "selected_tool": "web_search"},
            task_state={"available_actions": ["tool_call", "workflow_call", "respond"]},
            retrieval_hits=[],
            tool_candidates=[{"tool_name": "web_search", "requires_approval": False, "risk_level": "low"}],
            confirmed=False,
            episodes=[
                {
                    "episode_id": "ep-retry",
                    "chosen_strategy": "tool_call",
                    "outcome_status": "FAILED_RETRYABLE",
                    "tool_names": ["web_search"],
                    "useful_lessons": ["Retryable failures should escalate into a different action type."],
                }
            ],
            has_retrieval_observation=False,
            latest_result=None,
            requested_mode="auto",
        )

        self.assertEqual("workflow_call", action["action_type"])
        self.assertEqual(1, policy["experience_profile"]["tool_retry_failures"])

    def test_choose_next_action_respects_policy_memory_for_unstable_tool(self) -> None:
        action, policy = choose_next_action(
            goal={"normalized_goal": "search docs", "unknowns": [], "risk_level": "low"},
            planner={
                "action": "use_tool",
                "selected_tool": "web_search",
                "policy_signals": {
                    "action_signal": "tool_call",
                    "action_affinities": {"tool_call": 0.85, "workflow_call": 0.45},
                },
            },
            task_state={
                "available_actions": ["tool_call", "workflow_call", "respond"],
                "policy_memory": {
                    "version_id": "policy-v2",
                    "action_bias": {"workflow_call": 3, "tool_call": 1},
                    "tool_failure_counts": {"web_search": 2},
                    "memory_hygiene": {"memory_confidence": 0.8},
                },
            },
            retrieval_hits=[],
            tool_candidates=[{"tool_name": "web_search", "requires_approval": False, "risk_level": "low"}],
            confirmed=False,
            episodes=[],
            has_retrieval_observation=False,
            latest_result=None,
            requested_mode="auto",
        )

        self.assertEqual("workflow_call", action["action_type"])
        self.assertEqual("policy-v2", policy["policy_version_id"])

    def test_choose_next_action_prefers_reliable_tool_when_memory_confidence_is_high(self) -> None:
        action, policy = choose_next_action(
            goal={"normalized_goal": "search docs", "unknowns": [], "risk_level": "low"},
            planner={
                "action": "use_tool",
                "selected_tool": "web_search",
            },
            task_state={
                "available_actions": ["tool_call", "workflow_call", "respond"],
                "policy_memory": {
                    "version_id": "policy-v3",
                    "action_bias": {"tool_call": 2},
                    "tool_failure_counts": {"web_search": 1},
                    "tool_success_counts": {"web_search": 4},
                    "memory_hygiene": {"memory_confidence": 0.9},
                },
            },
            retrieval_hits=[],
            tool_candidates=[{"tool_name": "web_search", "requires_approval": False, "risk_level": "low"}],
            confirmed=False,
            episodes=[],
            has_retrieval_observation=False,
            latest_result=None,
            requested_mode="auto",
        )

        self.assertEqual("tool_call", action["action_type"])
        self.assertEqual(0.9, policy["memory_confidence"])
        self.assertEqual(4, policy["selected_tool_memory"]["successes"])

    def test_choose_next_action_uses_tool_reliability_signal(self) -> None:
        action, policy = choose_next_action(
            goal={"normalized_goal": "search docs", "unknowns": [], "risk_level": "low"},
            planner={"action": "use_tool", "selected_tool": "web_search"},
            task_state={
                "available_actions": ["tool_call", "workflow_call", "respond"],
                "policy_memory": {
                    "version_id": "policy-v5",
                    "tool_reliability": {
                        "web_search": {"score": 0.72, "confidence": 0.7, "evidence": 4}
                    },
                    "memory_hygiene": {"memory_confidence": 0.5},
                },
            },
            retrieval_hits=[],
            tool_candidates=[{"tool_name": "web_search", "requires_approval": False, "risk_level": "low"}],
            confirmed=False,
            episodes=[],
            has_retrieval_observation=False,
            latest_result=None,
            requested_mode="auto",
        )

        self.assertEqual("tool_call", action["action_type"])
        self.assertGreater(policy["selected_tool_memory"]["reliability_score"], 0.0)

    def test_choose_next_action_avoids_negative_tool_reliability(self) -> None:
        action, policy = choose_next_action(
            goal={"normalized_goal": "search docs", "unknowns": [], "risk_level": "low"},
            planner={"action": "use_tool", "selected_tool": "web_search"},
            task_state={
                "available_actions": ["tool_call", "workflow_call", "respond"],
                "policy_memory": {
                    "version_id": "policy-v6",
                    "tool_reliability": {
                        "web_search": {"score": -0.55, "confidence": 0.8, "evidence": 5}
                    },
                    "memory_hygiene": {"memory_confidence": 0.6},
                },
            },
            retrieval_hits=[],
            tool_candidates=[{"tool_name": "web_search", "requires_approval": False, "risk_level": "low"}],
            confirmed=False,
            episodes=[],
            has_retrieval_observation=False,
            latest_result=None,
            requested_mode="auto",
        )

        self.assertEqual("workflow_call", action["action_type"])
        self.assertLess(policy["selected_tool_memory"]["reliability_score"], 0.0)

    def test_choose_next_action_ignores_low_confidence_tool_failure_memory(self) -> None:
        action, policy = choose_next_action(
            goal={"normalized_goal": "search docs", "unknowns": [], "risk_level": "low"},
            planner={
                "action": "use_tool",
                "selected_tool": "web_search",
                "policy_signals": {
                    "action_signal": "tool_call",
                    "action_affinities": {"tool_call": 0.9, "workflow_call": 0.2},
                },
            },
            task_state={
                "available_actions": ["tool_call", "workflow_call", "respond"],
                "policy_memory": {
                    "version_id": "policy-v4",
                    "action_bias": {"workflow_call": 5},
                    "tool_failure_counts": {"web_search": 1},
                    "memory_hygiene": {"memory_confidence": 0.2},
                },
            },
            retrieval_hits=[],
            tool_candidates=[{"tool_name": "web_search", "requires_approval": False, "risk_level": "low"}],
            confirmed=False,
            episodes=[],
            has_retrieval_observation=False,
            latest_result=None,
            requested_mode="auto",
        )

        self.assertEqual("tool_call", action["action_type"])
        self.assertEqual(1, policy["selected_tool_memory"]["failures"])

    def test_choose_next_action_keeps_low_risk_tool_request_on_fast_path_despite_workflow_history(self) -> None:
        action, policy = choose_next_action(
            goal={"normalized_goal": "请帮我搜索 workflow 文档", "unknowns": [], "risk_level": "low"},
            planner={
                "action": "use_tool",
                "selected_tool": "web_search",
                "policy_signals": {
                    "action_signal": "tool_call",
                    "action_affinities": {"tool_call": 0.68, "workflow_call": 0.2},
                },
                "intent": "knowledge_lookup",
            },
            task_state={"available_actions": ["tool_call", "workflow_call", "respond"]},
            retrieval_hits=[],
            tool_candidates=[{"tool_name": "web_search", "requires_approval": False, "risk_level": "low"}],
            confirmed=False,
            episodes=[
                {
                    "episode_id": "ep-workflow",
                    "chosen_strategy": "workflow_call",
                    "outcome_status": "SUCCEEDED",
                }
            ],
            has_retrieval_observation=False,
            latest_result=None,
            requested_mode="auto",
        )

        self.assertEqual("tool_call", action["action_type"])
        self.assertEqual("tool_call", policy["selected_action"])

    def test_score_goal_portfolio_entry_boosts_dynamic_stalled_goal(self) -> None:
        portfolio = score_goal_portfolio_entry(
            {
                "goal_id": "goal-1",
                "continuation_count": 3,
                "updated_at": "2026-03-15T00:00:00+00:00",
                "goal_state": {
                    "agenda": {"priority_score": 0.62, "ready_count": 1, "blocked_count": 2, "active_subgoal_kind": "dynamic"},
                    "wake_condition": {"kind": "scheduler_cooldown"},
                    "active_subgoal": {"kind": "dynamic"},
                    "policy": {
                        "selected_action": "replan",
                        "policy_memory": {
                            "portfolio_bias": {
                                "dynamic_subgoal_boost": 2,
                                "stalled_goal_boost": 3,
                                "replan_goal_boost": 1,
                            }
                            ,
                            "portfolio_learning": {
                                "scheduler_confidence": 0.75,
                                "preempt_success_rate": 0.7,
                                "preempt_regret_rate": 0.05,
                            }
                        },
                    },
                    "wake_graph": {"waiting_events": []},
                },
            },
            active_goal_count=4,
            max_active_goals=4,
            soft_preempt_threshold=0.85,
        )

        self.assertGreaterEqual(portfolio["portfolio_score"], 0.85)
        self.assertTrue(portfolio["soft_preempt"])
        self.assertGreater(portfolio["allocated_budget"], 1.5)

    def test_score_goal_portfolio_entry_penalizes_unreliable_external_source(self) -> None:
        portfolio = score_goal_portfolio_entry(
            {
                "goal_id": "goal-external",
                "updated_at": "2026-03-15T00:00:00+00:00",
                "goal_state": {
                    "agenda": {"priority_score": 0.7},
                    "wake_condition": {"kind": "external_signal", "source": "vendor_webhook"},
                    "policy": {
                        "selected_action": "workflow_call",
                        "policy_memory": {
                            "external_source_reliability": {
                                "vendor_webhook": {"score": -0.8, "confidence": 0.9},
                            }
                        },
                    },
                    "wake_graph": {"waiting_events": [{"kind": "external_signal"}]},
                },
            },
            active_goal_count=1,
            max_active_goals=3,
        )

        self.assertGreater(portfolio["external_wait_penalty"], 0.0)
        self.assertTrue(any("source_reliability=" in item for item in portfolio["rationale"]))

    def test_score_goal_portfolio_entry_prefers_topic_specific_source_reliability(self) -> None:
        portfolio = score_goal_portfolio_entry(
            {
                "goal_id": "goal-topic-source",
                "updated_at": "2026-03-15T00:00:00+00:00",
                "goal_state": {
                    "agenda": {"priority_score": 0.62},
                    "wake_condition": {
                        "kind": "external_signal",
                        "source": "vendor_webhook",
                        "event_topic": "completed",
                    },
                    "policy": {
                        "selected_action": "workflow_call",
                        "policy_memory": {
                            "external_source_reliability": {
                                "vendor_webhook": {"score": -0.9, "confidence": 0.95},
                                "vendor_webhook:topic:completed": {"score": 0.7, "confidence": 0.95},
                            }
                        },
                    },
                    "wake_graph": {"waiting_events": [{"kind": "external_signal"}]},
                },
            },
            active_goal_count=1,
            max_active_goals=3,
        )

        self.assertEqual(0.0, portfolio["external_wait_penalty"])
        self.assertGreater(portfolio["portfolio_score"], 0.62)
        self.assertTrue(any("topic=completed" in item for item in portfolio["rationale"]))

    def test_score_goal_portfolio_entry_uses_starvation_and_wait_learning(self) -> None:
        portfolio = score_goal_portfolio_entry(
            {
                "goal_id": "goal-learning",
                "updated_at": "2026-03-15T00:00:00+00:00",
                "continuation_count": 1,
                "goal_state": {
                    "agenda": {"priority_score": 0.66, "ready_count": 1},
                    "wake_condition": {"kind": "external_signal", "source": "vendor_webhook"},
                    "policy": {
                        "selected_action": "workflow_call",
                        "policy_memory": {
                            "portfolio_learning": {
                                "starvation_rate": 0.4,
                                "subscription_timeout_rate": 0.35,
                                "external_wait_success_rate": 0.55,
                                "external_wait_failure_rate": 0.15,
                                "portfolio_throughput_score": 0.72,
                            },
                            "external_source_reliability": {
                                "vendor_webhook": {"score": 0.4, "confidence": 0.8},
                            },
                        },
                    },
                    "wake_graph": {"waiting_events": [{"kind": "external_signal"}]},
                },
            },
            active_goal_count=1,
            max_active_goals=3,
        )

        self.assertGreater(portfolio["portfolio_score"], 0.66)
        self.assertGreater(portfolio["external_wait_penalty"], 0.0)
        self.assertTrue(any("starvation_rate=" in item for item in portfolio["rationale"]))
        self.assertTrue(any("throughput=" in item for item in portfolio["rationale"]))

    def test_select_goal_portfolio_slice_prefers_highest_priority_goal(self) -> None:
        plan = select_goal_portfolio_slice(
            [
                {
                    "goal_id": "goal-low",
                    "updated_at": "2026-03-15T10:00:00+00:00",
                    "goal_state": {
                        "agenda": {"priority_score": 0.35},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "policy": {"selected_action": "workflow_call"},
                    },
                },
                {
                    "goal_id": "goal-high",
                    "updated_at": "2026-03-15T09:00:00+00:00",
                    "goal_state": {
                        "agenda": {"priority_score": 0.82, "active_subgoal_kind": "dynamic"},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "active_subgoal": {"kind": "dynamic"},
                        "policy": {"selected_action": "replan"},
                    },
                },
            ],
            active_goal_count=0,
            max_active_goals=1,
            dispatch_limit=1,
        )

        self.assertEqual("goal-high", plan["selected"][0]["goal_id"])
        self.assertEqual("dispatch", plan["selected"][0]["portfolio"]["dispatch_decision"])
        self.assertEqual("dispatch_limit_reached", plan["deferred"][0]["portfolio"]["dispatch_reason"])

    def test_select_goal_portfolio_slice_uses_policy_memory_override_for_external_source_learning(self) -> None:
        plan = select_goal_portfolio_slice(
            [
                {
                    "goal_id": "goal-external",
                    "updated_at": "2026-03-15T10:00:00+00:00",
                    "goal_state": {
                        "agenda": {"priority_score": 0.81},
                        "wake_condition": {"kind": "external_signal", "source": "vendor_webhook"},
                        "policy": {"selected_action": "workflow_call"},
                        "wake_graph": {"waiting_events": [{"kind": "external_signal"}]},
                    },
                },
                {
                    "goal_id": "goal-local",
                    "updated_at": "2026-03-15T09:00:00+00:00",
                    "goal_state": {
                        "agenda": {"priority_score": 0.74},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "policy": {"selected_action": "workflow_call"},
                    },
                },
            ],
            active_goal_count=0,
            max_active_goals=1,
            dispatch_limit=1,
            policy_memory_override={
                "external_source_reliability": {
                    "vendor_webhook": {"score": -0.95, "confidence": 0.95},
                }
            },
        )

        self.assertEqual("goal-local", plan["selected"][0]["goal_id"])

    def test_recommend_goal_holds_targets_lowest_value_live_goal(self) -> None:
        holds = recommend_goal_holds(
            [
                {
                    "tenant_id": "default",
                    "goal_id": "goal-routine",
                    "updated_at": "2026-03-15T08:00:00+00:00",
                    "goal_state": {
                        "agenda": {"priority_score": 0.21},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "current_action": {"action_type": "workflow_call"},
                        "policy": {"selected_action": "workflow_call"},
                    },
                },
                {
                    "tenant_id": "default",
                    "goal_id": "goal-valuable",
                    "updated_at": "2026-03-15T05:00:00+00:00",
                    "goal_state": {
                        "agenda": {"priority_score": 0.74},
                        "wake_condition": {"kind": "scheduler_cooldown"},
                        "active_subgoal": {"kind": "dynamic"},
                        "current_action": {"action_type": "workflow_call"},
                        "policy": {"selected_action": "workflow_call"},
                    },
                },
            ],
            selected_entries=[
                {
                    "goal_id": "goal-urgent",
                    "portfolio": {"soft_preempt": True},
                }
            ],
            active_goal_count=4,
            max_active_goals=4,
            hold_seconds=120,
        )

        self.assertEqual("goal-routine", holds[0]["goal_id"])
        self.assertEqual("HELD", holds[0]["hold_status"])
        self.assertEqual("goal-urgent", holds[0]["held_by_goal_id"])


if __name__ == "__main__":
    unittest.main()
