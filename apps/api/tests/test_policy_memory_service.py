import unittest
from unittest.mock import patch

from app.config import settings
from app.services.policy_memory_service import (
    compare_eval_summaries,
    derive_policy_eval_summary,
    maybe_auto_evaluate_candidate_policy,
    record_episode_feedback,
    record_external_signal_feedback,
    record_policy_eval,
    record_portfolio_feedback,
    record_shadow_portfolio_probe,
    record_shadow_portfolio_outcome,
    record_shadow_policy_probe,
    record_shadow_policy_outcome,
    select_runtime_policy_version,
)


class _FakePolicyRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.active_id: str | None = None
        self.candidate_id: str | None = None
        self.canary_id: str | None = None
        self.eval_rows: list[dict] = []

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

    def create_eval_run(
        self,
        *,
        tenant_id: str,
        eval_run_id: str,
        candidate_version_id: str,
        baseline_version_id: str,
        summary: dict,
        verdict: dict,
        created_by: str | None,
    ):
        del tenant_id, created_by
        row = {
            "eval_run_id": eval_run_id,
            "candidate_version_id": candidate_version_id,
            "baseline_version_id": baseline_version_id,
            "summary": dict(summary),
            "verdict": dict(verdict),
        }
        self.eval_rows.append(row)
        return row

    def activate_policy_version(self, *, tenant_id: str, version_id: str, actor_user_id: str | None, rollback: bool = False):
        del tenant_id, actor_user_id, rollback
        if self.active_id and self.active_id in self.rows:
            self.rows[self.active_id]["status"] = "ARCHIVED"
        self.active_id = version_id
        self.rows[version_id]["status"] = "ACTIVE"
        if self.candidate_id == version_id:
            self.candidate_id = None
        if self.canary_id == version_id:
            self.canary_id = None

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


class PolicyMemoryServiceTests(unittest.TestCase):
    def test_episode_feedback_builds_candidate_memory_version(self) -> None:
        repo = _FakePolicyRepo()
        candidate = record_episode_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            episode={
                "chosen_strategy": "tool_call",
                "outcome_status": "FAILED_RETRYABLE",
                "tool_names": ["web_search"],
                "useful_lessons": ["Retryable failures should escalate into a different action type."],
                "episode_payload": {
                    "outcome_signal": {
                        "latest_result": {"status": "FAILED_RETRYABLE"},
                    }
                },
            },
        )

        self.assertEqual("CANDIDATE", candidate["status"])
        memory_payload = candidate["memory_payload"]
        self.assertEqual(1, memory_payload["tool_failure_counts"]["web_search"])
        self.assertGreaterEqual(memory_payload["action_bias"]["workflow_call"], 1)
        self.assertGreaterEqual(memory_payload["portfolio_bias"]["dynamic_subgoal_boost"], 1)
        self.assertGreaterEqual(memory_payload["portfolio_bias"]["replan_goal_boost"], 1)
        self.assertIn(
            "Retryable failures should escalate into a different action type.",
            memory_payload["lesson_hints"],
        )
        self.assertGreaterEqual(memory_payload["eval_summary"]["agenda_stability"], 0.0)
        self.assertGreaterEqual(memory_payload["eval_summary"]["portfolio_goal_completion_rate"], 0.0)
        self.assertEqual(
            memory_payload["eval_summary"],
            candidate["comparison_payload"]["auto_eval_summary"],
        )

    def test_eval_comparison_rejects_regressions(self) -> None:
        verdict = compare_eval_summaries(
            active_summary={
                "success_rate": 0.95,
                "trace_coverage": 1.0,
                "prompt_leak_count": 0,
                "unauthorized_tool_calls": 0,
                "status_mismatch_count": 0,
                "portfolio_goal_completion_rate": 0.82,
                "preempt_recovery_success_rate": 0.78,
                "preempt_regret_rate": 0.04,
                "agenda_stability": 0.83,
            },
            candidate_summary={
                "success_rate": 0.91,
                "trace_coverage": 0.95,
                "prompt_leak_count": 1,
                "unauthorized_tool_calls": 0,
                "status_mismatch_count": 0,
                "portfolio_goal_completion_rate": 0.7,
                "preempt_recovery_success_rate": 0.62,
                "preempt_regret_rate": 0.19,
                "agenda_stability": 0.61,
            },
        )

        self.assertFalse(verdict["passed"])
        self.assertTrue(any("prompt leak" in reason for reason in verdict["reasons"]))
        self.assertTrue(any("preemption regret" in reason for reason in verdict["reasons"]))

    def test_portfolio_feedback_updates_candidate_memory(self) -> None:
        repo = _FakePolicyRepo()
        candidate = record_portfolio_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            feedback={
                "event_kind": "preempt_cancel",
                "goal_id": "goal-urgent",
                "held_goal_id": "goal-routine",
                "urgency_score": 0.93,
            },
        )

        memory_payload = candidate["memory_payload"]
        self.assertGreaterEqual(memory_payload["portfolio_bias"]["continuation_penalty"], 2)
        self.assertGreaterEqual(memory_payload["portfolio_bias"]["stalled_goal_boost"], 1)
        self.assertGreaterEqual(memory_payload["portfolio_outcomes"]["preempt_cancel_events"], 1)
        self.assertEqual("portfolio", candidate["comparison_payload"]["last_feedback"]["kind"])
        self.assertGreaterEqual(memory_payload["eval_summary"]["preempt_regret_rate"], 0.0)

    def test_portfolio_feedback_tracks_resume_success(self) -> None:
        repo = _FakePolicyRepo()
        candidate = record_portfolio_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            feedback={
                "event_kind": "preempt_resume_success",
                "goal_id": "goal-resume",
                "held_goal_id": "goal-urgent",
                "urgency_score": 0.76,
            },
        )

        memory_payload = candidate["memory_payload"]
        self.assertGreaterEqual(memory_payload["portfolio_outcomes"]["preempt_resume_success"], 1)
        self.assertGreaterEqual(memory_payload["eval_summary"]["preempt_recovery_success_rate"], 1.0)

    def test_derive_policy_eval_summary_builds_portfolio_metrics(self) -> None:
        summary = derive_policy_eval_summary(
            memory_payload={
                "critic_patterns": {
                    "SUCCEEDED": 4,
                    "FAILED_RETRYABLE": 1,
                    "TIMED_OUT": 1,
                },
                "portfolio_bias": {
                    "continuation_penalty": 2,
                },
                "portfolio_outcomes": {
                    "hold_events": 3,
                    "preempt_cancel_events": 2,
                    "preempt_resume_success": 2,
                    "preempt_resume_regret": 1,
                },
                "feedback_counts": {
                    "episodes": 6,
                    "eval_runs": 1,
                    "portfolio_events": 5,
                },
            },
            base_summary={"trace_coverage": 0.94},
        )

        self.assertAlmostEqual(4 / 6, summary["success_rate"], places=2)
        self.assertGreater(summary["portfolio_goal_completion_rate"], 0.0)
        self.assertGreater(summary["preempt_recovery_success_rate"], 0.0)
        self.assertGreater(summary["agenda_stability"], 0.0)
        self.assertEqual(0.94, summary["trace_coverage"])
        self.assertEqual(6, summary["feedback_episode_count"])

    def test_record_policy_eval_merges_derived_portfolio_metrics(self) -> None:
        repo = _FakePolicyRepo()
        record_episode_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            episode={
                "chosen_strategy": "workflow_call",
                "outcome_status": "SUCCEEDED",
                "tool_names": [],
                "useful_lessons": ["Workflow continuation completed after replanning."],
                "episode_payload": {
                    "outcome_signal": {
                        "latest_result": {"status": "SUCCEEDED"},
                    }
                },
            },
        )
        candidate = record_portfolio_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            feedback={
                "event_kind": "preempt_resume_success",
                "goal_id": "goal-resume",
                "held_goal_id": "goal-urgent",
                "urgency_score": 0.8,
            },
        )

        result = record_policy_eval(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            candidate_version_id=str(candidate["version_id"]),
            summary={
                "success_rate": 0.96,
                "trace_coverage": 0.99,
                "prompt_leak_count": 0,
                "unauthorized_tool_calls": 0,
                "status_mismatch_count": 0,
            },
            auto_promote=False,
        )

        self.assertFalse(result["promoted"])
        self.assertEqual(1, len(repo.eval_rows))
        eval_summary = repo.eval_rows[0]["summary"]
        self.assertIn("portfolio_goal_completion_rate", eval_summary)
        self.assertIn("preempt_recovery_success_rate", eval_summary)
        self.assertIn("agenda_stability", eval_summary)
        self.assertEqual(
            eval_summary,
            repo.rows[str(candidate["version_id"])]["comparison_payload"]["last_eval_summary"],
        )

    def test_episode_feedback_applies_memory_hygiene_and_confidence(self) -> None:
        repo = _FakePolicyRepo()
        with (
            patch.object(settings, "policy_memory_max_lessons", 3),
            patch.object(settings, "policy_memory_max_tool_entries", 2),
            patch.object(settings, "policy_memory_confidence_feedback_floor", 4),
            patch.object(settings, "policy_shadow_enabled", False),
        ):
            for idx in range(5):
                candidate = record_episode_feedback(
                    repo=repo,
                    tenant_id="default",
                    actor_user_id="user-1",
                    episode={
                        "chosen_strategy": "tool_call",
                        "outcome_status": "SUCCEEDED",
                        "tool_names": [f"tool-{idx}", "tool-core"],
                        "useful_lessons": [f"lesson-{idx}"],
                        "episode_payload": {
                            "outcome_signal": {
                                "latest_result": {"status": "SUCCEEDED"},
                            }
                        },
                    },
                )

        memory_payload = candidate["memory_payload"]
        self.assertLessEqual(len(memory_payload["lesson_hints"]), 3)
        self.assertEqual(3, len(memory_payload["lesson_catalog"]))
        self.assertEqual(2, len(memory_payload["tool_success_counts"]))
        self.assertEqual(1.0, memory_payload["memory_hygiene"]["memory_confidence"])
        self.assertNotIn("lesson-0", memory_payload["lesson_hints"])
        self.assertNotIn("tool-0", memory_payload["tool_success_counts"])
        self.assertEqual(
            1.0,
            memory_payload["eval_summary"]["memory_confidence"],
        )

    def test_memory_hygiene_tracks_conflicting_tool_memory(self) -> None:
        repo = _FakePolicyRepo()
        with patch.object(settings, "policy_shadow_enabled", False):
            candidate = record_episode_feedback(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                episode={
                    "chosen_strategy": "tool_call",
                    "outcome_status": "SUCCEEDED",
                    "tool_names": ["web_search"],
                    "useful_lessons": ["Direct tool call worked well."],
                    "episode_payload": {"outcome_signal": {"latest_result": {"status": "SUCCEEDED"}}},
                },
            )
            candidate = record_episode_feedback(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                episode={
                    "chosen_strategy": "tool_call",
                    "outcome_status": "FAILED_RETRYABLE",
                    "tool_names": ["web_search"],
                    "useful_lessons": ["Retryable failures should escalate into a different action type."],
                    "episode_payload": {"outcome_signal": {"latest_result": {"status": "FAILED_RETRYABLE"}}},
                },
            )

        hygiene = candidate["memory_payload"]["memory_hygiene"]
        self.assertGreaterEqual(hygiene["tool_conflict_count"], 1)
        self.assertGreaterEqual(candidate["memory_payload"]["eval_summary"]["memory_conflict_count"], 1)
        self.assertLess(hygiene["memory_confidence"], 1.0)

    def test_memory_hygiene_forgets_stale_low_support_lessons_and_tools(self) -> None:
        repo = _FakePolicyRepo()
        with (
            patch.object(settings, "policy_shadow_enabled", False),
            patch.object(settings, "policy_memory_forget_after_updates", 1),
            patch.object(settings, "policy_memory_min_tool_evidence", 1),
            patch.object(settings, "policy_memory_max_retired_lessons", 4),
        ):
            record_episode_feedback(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                episode={
                    "chosen_strategy": "tool_call",
                    "outcome_status": "SUCCEEDED",
                    "tool_names": ["tool-stale"],
                    "useful_lessons": ["stale lesson"],
                    "episode_payload": {"outcome_signal": {"latest_result": {"status": "SUCCEEDED"}}},
                },
            )
            record_episode_feedback(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                episode={
                    "chosen_strategy": "workflow_call",
                    "outcome_status": "SUCCEEDED",
                    "tool_names": ["tool-fresh"],
                    "useful_lessons": ["fresh lesson"],
                    "episode_payload": {"outcome_signal": {"latest_result": {"status": "SUCCEEDED"}}},
                },
            )
            candidate = record_episode_feedback(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                episode={
                    "chosen_strategy": "respond",
                    "outcome_status": "SUCCEEDED",
                    "tool_names": [],
                    "useful_lessons": ["latest lesson"],
                    "episode_payload": {"outcome_signal": {"latest_result": {"status": "SUCCEEDED"}}},
                },
            )
            candidate = record_episode_feedback(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                episode={
                    "chosen_strategy": "respond",
                    "outcome_status": "SUCCEEDED",
                    "tool_names": [],
                    "useful_lessons": ["newest lesson"],
                    "episode_payload": {"outcome_signal": {"latest_result": {"status": "SUCCEEDED"}}},
                },
            )

        memory_payload = candidate["memory_payload"]
        self.assertNotIn("stale lesson", memory_payload["lesson_hints"])
        self.assertTrue(any(item["lesson"] == "stale lesson" for item in memory_payload["retired_lessons"]))
        self.assertNotIn("tool-stale", memory_payload["tool_success_counts"])
        self.assertGreaterEqual(memory_payload["memory_hygiene"]["forgotten_lessons"], 1)
        self.assertGreaterEqual(memory_payload["memory_hygiene"]["forgotten_tools"], 1)

    def test_memory_hygiene_computes_tool_reliability_scores(self) -> None:
        repo = _FakePolicyRepo()
        with patch.object(settings, "policy_shadow_enabled", False):
            candidate = record_episode_feedback(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                episode={
                    "chosen_strategy": "tool_call",
                    "outcome_status": "SUCCEEDED",
                    "tool_names": ["web_search"],
                    "useful_lessons": ["Tool worked."],
                    "episode_payload": {"outcome_signal": {"latest_result": {"status": "SUCCEEDED"}}},
                },
            )
            candidate = record_episode_feedback(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                episode={
                    "chosen_strategy": "tool_call",
                    "outcome_status": "SUCCEEDED",
                    "tool_names": ["web_search"],
                    "useful_lessons": ["Tool worked again."],
                    "episode_payload": {"outcome_signal": {"latest_result": {"status": "SUCCEEDED"}}},
                },
            )

        tool_reliability = candidate["memory_payload"]["tool_reliability"]["web_search"]
        self.assertGreater(tool_reliability["score"], 0.0)
        self.assertGreater(tool_reliability["confidence"], 0.0)
        self.assertGreaterEqual(candidate["memory_payload"]["eval_summary"]["tool_reliability_count"], 1)

    def test_portfolio_feedback_updates_portfolio_learning(self) -> None:
        repo = _FakePolicyRepo()
        candidate = record_portfolio_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            feedback={
                "event_kind": "preempt_resume_success",
                "goal_id": "goal-resume",
                "held_goal_id": "goal-held",
                "urgency_score": 0.88,
            },
        )
        candidate = record_portfolio_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            feedback={
                "event_kind": "hold",
                "goal_id": "goal-hold",
                "held_goal_id": "goal-background",
                "urgency_score": 0.52,
            },
        )

        learning = candidate["memory_payload"]["portfolio_learning"]
        self.assertGreaterEqual(learning["scheduler_confidence"], 0.0)
        self.assertGreater(learning["preempt_success_rate"], 0.0)
        self.assertGreater(candidate["memory_payload"]["eval_summary"]["portfolio_hold_adaptation_rate"], 0.0)

    def test_portfolio_feedback_tracks_wait_timeout_and_starvation_learning(self) -> None:
        repo = _FakePolicyRepo()
        candidate = record_portfolio_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            feedback={
                "event_kind": "subscription_timeout",
                "goal_id": "goal-timeout",
                "urgency_score": 0.71,
            },
        )
        candidate = record_portfolio_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            feedback={
                "event_kind": "external_wait_success",
                "goal_id": "goal-external",
                "urgency_score": 0.66,
            },
        )
        candidate = record_portfolio_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            feedback={
                "event_kind": "goal_starved",
                "goal_id": "goal-starved",
                "urgency_score": 0.83,
            },
        )

        learning = candidate["memory_payload"]["portfolio_learning"]
        self.assertGreater(learning["subscription_timeout_rate"], 0.0)
        self.assertGreater(learning["external_wait_success_rate"], 0.0)
        self.assertGreater(learning["starvation_rate"], 0.0)
        self.assertGreater(candidate["memory_payload"]["eval_summary"]["portfolio_external_wait_event_count"], 0)
        self.assertGreater(candidate["memory_payload"]["eval_summary"]["portfolio_starved_goal_count"], 0)

    def test_eval_comparison_rejects_starvation_and_timeout_regressions(self) -> None:
        verdict = compare_eval_summaries(
            active_summary={
                "success_rate": 0.95,
                "trace_coverage": 1.0,
                "prompt_leak_count": 0,
                "unauthorized_tool_calls": 0,
                "status_mismatch_count": 0,
                "portfolio_goal_completion_rate": 0.82,
                "preempt_recovery_success_rate": 0.78,
                "preempt_regret_rate": 0.04,
                "agenda_stability": 0.83,
                "portfolio_starvation_rate": 0.05,
                "portfolio_subscription_timeout_rate": 0.08,
                "portfolio_throughput_score": 0.74,
            },
            candidate_summary={
                "success_rate": 0.95,
                "trace_coverage": 1.0,
                "prompt_leak_count": 0,
                "unauthorized_tool_calls": 0,
                "status_mismatch_count": 0,
                "portfolio_goal_completion_rate": 0.82,
                "preempt_recovery_success_rate": 0.78,
                "preempt_regret_rate": 0.04,
                "agenda_stability": 0.83,
                "portfolio_starvation_rate": 0.24,
                "portfolio_subscription_timeout_rate": 0.29,
                "portfolio_throughput_score": 0.46,
            },
        )

        self.assertFalse(verdict["passed"])
        self.assertTrue(any("starvation" in reason for reason in verdict["reasons"]))
        self.assertTrue(any("timeout" in reason for reason in verdict["reasons"]))
        self.assertTrue(any("throughput" in reason for reason in verdict["reasons"]))

    def test_external_signal_feedback_tracks_source_reliability(self) -> None:
        repo = _FakePolicyRepo()
        candidate = record_external_signal_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            feedback={
                "source": "vendor_webhook",
                "event_topic": "completed",
                "adapter_outcome": "failure",
                "requires_replan": True,
                "matched_goal_count": 1,
            },
        )
        candidate = record_external_signal_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            feedback={
                "source": "vendor_webhook",
                "event_topic": "completed",
                "adapter_outcome": "success",
                "requires_replan": False,
                "matched_goal_count": 1,
            },
        )

        memory_payload = candidate["memory_payload"]
        self.assertEqual(1, memory_payload["external_signal_outcomes"]["failure"])
        self.assertEqual(1, memory_payload["external_signal_outcomes"]["success"])
        self.assertIn("vendor_webhook", memory_payload["external_source_reliability"])
        self.assertIn("vendor_webhook:topic:completed", memory_payload["external_source_reliability"])
        self.assertGreaterEqual(memory_payload["eval_summary"]["feedback_external_signal_count"], 2)
        self.assertGreaterEqual(memory_payload["eval_summary"]["external_source_reliability_count"], 2)

    def test_record_shadow_policy_probe_tracks_agreement_rates(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.97}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        updated = record_shadow_policy_probe(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            candidate_version_id="policy-canary",
            probe={
                "live_mode": "active",
                "live_policy_version_id": "policy-active",
                "live_action": "workflow_call",
                "live_route": "workflow_task",
                "shadow_policy_version_id": "policy-canary",
                "shadow_action": "tool_call",
                "shadow_route": "tool_task",
                "risk_level": "high",
                "goal_id": "goal-shadow",
                "conversation_id": "conv-shadow",
            },
        )

        comparison_payload = updated["comparison_payload"]
        self.assertEqual(1, comparison_payload["shadow_probe_counts"]["total"])
        self.assertEqual(1, comparison_payload["shadow_probe_counts"]["action_divergence"])
        self.assertEqual(1, comparison_payload["shadow_probe_counts"]["high_risk_total"])
        self.assertEqual(
            0.0,
            comparison_payload["shadow_eval_summary"]["shadow_action_agreement_rate"],
        )
        self.assertEqual(
            0.0,
            comparison_payload["shadow_eval_summary"]["shadow_high_risk_action_agreement_rate"],
        )

    def test_record_policy_eval_merges_shadow_probe_metrics(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
            tenant_id="default",
            version_id="policy-active",
            version_tag="active",
            status="ACTIVE",
            base_version_id=None,
            source="bootstrap",
            memory_payload={
                "eval_summary": {
                    "success_rate": 0.95,
                    "trace_coverage": 1.0,
                    "prompt_leak_count": 0,
                    "unauthorized_tool_calls": 0,
                    "status_mismatch_count": 0,
                }
            },
            comparison_payload={},
            created_by="user-1",
        )
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.97}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        record_shadow_policy_probe(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            candidate_version_id="policy-canary",
            probe={
                "live_mode": "active",
                "live_policy_version_id": "policy-active",
                "live_action": "workflow_call",
                "live_route": "workflow_task",
                "shadow_policy_version_id": "policy-canary",
                "shadow_action": "workflow_call",
                "shadow_route": "workflow_task",
                "risk_level": "low",
                "goal_id": "goal-shadow-1",
                "conversation_id": "conv-shadow-1",
            },
        )
        record_shadow_policy_probe(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            candidate_version_id="policy-canary",
            probe={
                "live_mode": "active",
                "live_policy_version_id": "policy-active",
                "live_action": "workflow_call",
                "live_route": "workflow_task",
                "shadow_policy_version_id": "policy-canary",
                "shadow_action": "workflow_call",
                "shadow_route": "workflow_task",
                "risk_level": "high",
                "goal_id": "goal-shadow-2",
                "conversation_id": "conv-shadow-2",
            },
        )
        record_shadow_policy_probe(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            candidate_version_id="policy-canary",
            probe={
                "live_mode": "active",
                "live_policy_version_id": "policy-active",
                "live_action": "workflow_call",
                "live_route": "workflow_task",
                "shadow_policy_version_id": "policy-canary",
                "shadow_action": "workflow_call",
                "shadow_route": "workflow_task",
                "risk_level": "low",
                "goal_id": "goal-shadow-3",
                "conversation_id": "conv-shadow-3",
            },
        )

        with (
            patch.object(settings, "policy_shadow_enabled", True),
            patch.object(settings, "policy_shadow_min_probe_count", 2),
            patch.object(settings, "policy_shadow_min_high_risk_probe_count", 1),
            patch.object(settings, "policy_shadow_min_action_agreement_rate", 0.7),
            patch.object(settings, "policy_shadow_min_high_risk_action_agreement_rate", 0.8),
        ):
            result = record_policy_eval(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                candidate_version_id="policy-canary",
                summary={
                    "success_rate": 0.97,
                    "trace_coverage": 1.0,
                    "prompt_leak_count": 0,
                    "unauthorized_tool_calls": 0,
                    "status_mismatch_count": 0,
                },
                auto_promote=False,
            )

        self.assertTrue(result["verdict"]["passed"])
        eval_summary = repo.eval_rows[0]["summary"]
        self.assertEqual(3, eval_summary["shadow_probe_count"])
        self.assertEqual(1.0, eval_summary["shadow_action_agreement_rate"])
        self.assertEqual(1.0, eval_summary["shadow_high_risk_action_agreement_rate"])

    def test_shadow_probe_can_auto_rollback_canary(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.97}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_shadow_enabled", True),
            patch.object(settings, "policy_shadow_auto_rollback_enabled", True),
            patch.object(settings, "policy_shadow_min_probe_count", 2),
            patch.object(settings, "policy_shadow_min_action_agreement_rate", 0.8),
        ):
            record_shadow_policy_probe(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                candidate_version_id="policy-canary",
                probe={
                    "live_mode": "active",
                    "live_policy_version_id": "policy-active",
                    "live_action": "workflow_call",
                    "live_route": "workflow_task",
                    "shadow_policy_version_id": "policy-canary",
                    "shadow_action": "tool_call",
                    "shadow_route": "tool_task",
                    "risk_level": "low",
                    "goal_id": "goal-shadow-a",
                    "conversation_id": "conv-shadow-a",
                },
            )
            updated = record_shadow_policy_probe(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                candidate_version_id="policy-canary",
                probe={
                    "live_mode": "active",
                    "live_policy_version_id": "policy-active",
                    "live_action": "workflow_call",
                    "live_route": "workflow_task",
                    "shadow_policy_version_id": "policy-canary",
                    "shadow_action": "tool_call",
                    "shadow_route": "tool_task",
                    "risk_level": "low",
                    "goal_id": "goal-shadow-b",
                    "conversation_id": "conv-shadow-b",
                },
            )

        self.assertEqual("ROLLED_BACK", updated["status"])
        self.assertFalse(updated["comparison_payload"]["shadow_guardrail"]["passed"])

    def test_record_shadow_portfolio_probe_tracks_agreement(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.97}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        updated = record_shadow_portfolio_probe(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            candidate_version_id="policy-canary",
            probe={
                "live_selected_goal_ids": ["goal-a"],
                "shadow_selected_goal_ids": ["goal-b"],
                "live_hold_goal_ids": ["goal-held-a"],
                "shadow_hold_goal_ids": ["goal-held-b"],
                "live_soft_preempt_goal_ids": ["goal-a"],
                "shadow_soft_preempt_goal_ids": [],
                "live_external_wait_sources": ["vendor_webhook"],
                "shadow_external_wait_sources": ["artifact_store"],
                "high_urgency": True,
            },
        )

        self.assertEqual(1, updated["comparison_payload"]["shadow_portfolio_counts"]["total"])
        self.assertEqual(1, updated["comparison_payload"]["shadow_portfolio_counts"]["divergent_total"])
        self.assertEqual(0.0, updated["comparison_payload"]["shadow_portfolio_summary"]["shadow_portfolio_agreement_rate"])
        self.assertEqual(0.0, updated["comparison_payload"]["shadow_portfolio_summary"]["shadow_portfolio_high_urgency_agreement_rate"])
        self.assertEqual(0.0, updated["comparison_payload"]["shadow_portfolio_summary"]["shadow_portfolio_external_wait_agreement_rate"])

    def test_record_shadow_policy_outcome_tracks_regret_signal(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.97}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        updated = record_shadow_policy_outcome(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            candidate_version_id="policy-canary",
            outcome={
                "goal_id": "goal-shadow-outcome",
                "conversation_id": "conv-shadow-outcome",
                "live_policy_version_id": "policy-active",
                "shadow_policy_version_id": "policy-canary",
                "live_action": "workflow_call",
                "shadow_action": "tool_call",
                "outcome_status": "SUCCEEDED",
                "risk_level": "low",
                "diverged": True,
            },
        )

        self.assertEqual(1, updated["comparison_payload"]["shadow_outcome_counts"]["total"])
        self.assertEqual(1, updated["comparison_payload"]["shadow_outcome_counts"]["live_success_divergent"])
        self.assertEqual(
            1.0,
            updated["comparison_payload"]["shadow_outcome_summary"]["shadow_regret_signal_rate"],
        )

    def test_record_shadow_portfolio_outcome_tracks_regret_signal(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.97}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        updated = record_shadow_portfolio_outcome(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            candidate_version_id="policy-canary",
            outcome={
                "goal_id": "goal-live",
                "conversation_id": "conv-live",
                "live_policy_version_id": "policy-active",
                "shadow_policy_version_id": "policy-canary",
                "live_goal_id": "goal-live",
                "shadow_selected_goal_ids": ["goal-shadow"],
                "outcome_status": "SUCCEEDED",
                "diverged": True,
                "high_urgency": True,
                "live_external_wait_sources": ["vendor_webhook"],
                "shadow_external_wait_sources": [],
            },
        )

        self.assertEqual(1, updated["comparison_payload"]["shadow_portfolio_outcome_counts"]["total"])
        self.assertEqual(1, updated["comparison_payload"]["shadow_portfolio_outcome_counts"]["divergent_total"])
        self.assertEqual(1, updated["comparison_payload"]["shadow_portfolio_outcome_counts"]["live_success_divergent"])
        self.assertEqual(
            1.0,
            updated["comparison_payload"]["shadow_portfolio_outcome_summary"]["shadow_portfolio_regret_signal_rate"],
        )
        self.assertEqual(
            1.0,
            updated["comparison_payload"]["shadow_portfolio_outcome_summary"]["shadow_portfolio_external_wait_regret_signal_rate"],
        )

    def test_shadow_outcome_can_auto_rollback_canary(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.97}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_shadow_auto_rollback_enabled", True),
            patch.object(settings, "policy_shadow_min_outcome_count", 2),
            patch.object(settings, "policy_shadow_max_regret_signal_rate", 0.4),
        ):
            first = record_shadow_policy_outcome(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                candidate_version_id="policy-canary",
                outcome={
                    "goal_id": "goal-shadow-outcome-a",
                    "conversation_id": "conv-shadow-outcome-a",
                    "live_policy_version_id": "policy-active",
                    "shadow_policy_version_id": "policy-canary",
                    "live_action": "workflow_call",
                    "shadow_action": "tool_call",
                    "outcome_status": "SUCCEEDED",
                    "risk_level": "low",
                    "diverged": True,
                },
            )
            first_status = str(first["status"])
            first_guardrail = dict(first["comparison_payload"]["shadow_outcome_guardrail"])
            updated = record_shadow_policy_outcome(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                candidate_version_id="policy-canary",
                outcome={
                    "goal_id": "goal-shadow-outcome-b",
                    "conversation_id": "conv-shadow-outcome-b",
                    "live_policy_version_id": "policy-active",
                    "shadow_policy_version_id": "policy-canary",
                    "live_action": "workflow_call",
                    "shadow_action": "tool_call",
                    "outcome_status": "SUCCEEDED",
                    "risk_level": "low",
                    "diverged": True,
                },
            )

        self.assertEqual("CANARY", first_status)
        self.assertFalse(first_guardrail["ready"])
        self.assertEqual("ROLLED_BACK", updated["status"])
        self.assertTrue(updated["comparison_payload"]["shadow_outcome_guardrail"]["ready"])
        self.assertFalse(updated["comparison_payload"]["shadow_outcome_guardrail"]["passed"])

    def test_shadow_portfolio_probe_can_auto_rollback_canary(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.97}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_shadow_auto_rollback_enabled", True),
            patch.object(settings, "policy_shadow_min_portfolio_probe_count", 2),
            patch.object(settings, "policy_shadow_min_portfolio_agreement_rate", 0.7),
        ):
            first = record_shadow_portfolio_probe(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                candidate_version_id="policy-canary",
                probe={
                    "live_selected_goal_ids": ["goal-a"],
                    "shadow_selected_goal_ids": ["goal-b"],
                    "live_hold_goal_ids": [],
                    "shadow_hold_goal_ids": [],
                    "live_soft_preempt_goal_ids": ["goal-a"],
                    "shadow_soft_preempt_goal_ids": [],
                    "high_urgency": True,
                },
            )
            first_status = str(first["status"])
            updated = record_shadow_portfolio_probe(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                candidate_version_id="policy-canary",
                probe={
                    "live_selected_goal_ids": ["goal-a"],
                    "shadow_selected_goal_ids": ["goal-b"],
                    "live_hold_goal_ids": [],
                    "shadow_hold_goal_ids": [],
                    "live_soft_preempt_goal_ids": ["goal-a"],
                    "shadow_soft_preempt_goal_ids": [],
                    "high_urgency": True,
                },
            )

        self.assertEqual("CANARY", first_status)
        self.assertEqual("ROLLED_BACK", updated["status"])
        self.assertFalse(updated["comparison_payload"]["shadow_portfolio_guardrail"]["passed"])

    def test_shadow_portfolio_outcome_can_auto_rollback_canary(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.97}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_shadow_auto_rollback_enabled", True),
            patch.object(settings, "policy_shadow_min_outcome_count", 2),
            patch.object(settings, "policy_shadow_max_regret_signal_rate", 0.4),
        ):
            first = record_shadow_portfolio_outcome(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                candidate_version_id="policy-canary",
                outcome={
                    "goal_id": "goal-live-a",
                    "conversation_id": "conv-live-a",
                    "live_policy_version_id": "policy-active",
                    "shadow_policy_version_id": "policy-canary",
                    "live_goal_id": "goal-live-a",
                    "shadow_selected_goal_ids": ["goal-shadow-a"],
                    "outcome_status": "SUCCEEDED",
                    "diverged": True,
                },
            )
            first_status = str(first["status"])
            first_guardrail = dict(first["comparison_payload"]["shadow_portfolio_outcome_guardrail"])
            updated = record_shadow_portfolio_outcome(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                candidate_version_id="policy-canary",
                outcome={
                    "goal_id": "goal-live-b",
                    "conversation_id": "conv-live-b",
                    "live_policy_version_id": "policy-active",
                    "shadow_policy_version_id": "policy-canary",
                    "live_goal_id": "goal-live-b",
                    "shadow_selected_goal_ids": ["goal-shadow-b"],
                    "outcome_status": "SUCCEEDED",
                    "diverged": True,
                },
            )

        self.assertEqual("CANARY", first_status)
        self.assertFalse(first_guardrail["ready"])
        self.assertEqual("ROLLED_BACK", updated["status"])
        self.assertTrue(updated["comparison_payload"]["shadow_portfolio_outcome_guardrail"]["ready"])
        self.assertFalse(updated["comparison_payload"]["shadow_portfolio_outcome_guardrail"]["passed"])

    def test_canary_auto_eval_waits_for_shadow_outcome_floor(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={
                "eval_summary": {"success_rate": 0.97},
                "feedback_counts": {"episodes": 2, "portfolio_events": 0},
            },
            comparison_payload={
                "last_eval_verdict": {"passed": True},
                "shadow_outcome_counts": {"total": 1, "divergent_total": 0, "aligned_total": 1},
            },
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_auto_eval_enabled", True),
            patch.object(settings, "policy_auto_eval_min_episode_feedback", 2),
            patch.object(settings, "policy_auto_eval_min_portfolio_feedback", 0),
            patch.object(settings, "policy_auto_eval_min_total_feedback", 2),
            patch.object(settings, "policy_auto_eval_feedback_delta", 1),
            patch.object(settings, "policy_auto_eval_promote", True),
            patch.object(settings, "policy_shadow_enabled", True),
            patch.object(settings, "policy_shadow_min_probe_count", 0),
            patch.object(settings, "policy_shadow_min_outcome_count", 2),
        ):
            result = maybe_auto_evaluate_candidate_policy(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                candidate_version_id="policy-canary",
            )

        self.assertIsNone(result)
        self.assertEqual([], repo.eval_rows)


    def test_passing_eval_without_promote_moves_candidate_into_canary_stage(self) -> None:
        repo = _FakePolicyRepo()
        candidate = record_episode_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            episode={
                "chosen_strategy": "workflow_call",
                "outcome_status": "SUCCEEDED",
                "tool_names": [],
                "useful_lessons": ["Continuation completed cleanly after waiting on an external signal."],
                "episode_payload": {
                    "outcome_signal": {
                        "latest_result": {"status": "SUCCEEDED"},
                    }
                },
            },
        )

        with (
            patch.object(settings, "policy_eval_canary_on_pass_without_promote", True),
            patch.object(settings, "policy_shadow_enabled", False),
        ):
            result = record_policy_eval(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                candidate_version_id=str(candidate["version_id"]),
                summary={
                    "success_rate": 0.97,
                    "trace_coverage": 1.0,
                    "prompt_leak_count": 0,
                    "unauthorized_tool_calls": 0,
                    "status_mismatch_count": 0,
                },
                auto_promote=False,
            )

        self.assertEqual("CANARY", result["candidate_status"])
        self.assertEqual("CANARY", repo.rows[str(candidate["version_id"])]["status"])
        self.assertEqual(
            str(result["active_version_id"]),
            repo.rows[str(candidate["version_id"])]["comparison_payload"]["rollback_target_version_id"],
        )

        reused = record_portfolio_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            feedback={
                "event_kind": "hold",
                "goal_id": "goal-canary",
                "held_goal_id": "goal-routine",
                "urgency_score": 0.61,
            },
        )
        self.assertEqual(str(candidate["version_id"]), str(reused["version_id"]))

    def test_failing_eval_rolls_back_canary_candidate(self) -> None:
        repo = _FakePolicyRepo()
        candidate = record_episode_feedback(
            repo=repo,
            tenant_id="default",
            actor_user_id="user-1",
            episode={
                "chosen_strategy": "workflow_call",
                "outcome_status": "SUCCEEDED",
                "tool_names": [],
                "useful_lessons": ["Stable canary start."],
                "episode_payload": {
                    "outcome_signal": {
                        "latest_result": {"status": "SUCCEEDED"},
                    }
                },
            },
        )
        repo.mark_policy_version_status(tenant_id="default", version_id=str(candidate["version_id"]), status="CANARY")

        with patch.object(settings, "policy_canary_auto_rollback_on_failure", True):
            result = record_policy_eval(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                candidate_version_id=str(candidate["version_id"]),
                summary={
                    "success_rate": 0.82,
                    "trace_coverage": 0.94,
                    "prompt_leak_count": 1,
                    "unauthorized_tool_calls": 0,
                    "status_mismatch_count": 0,
                    "preempt_regret_rate": 0.4,
                },
                auto_promote=False,
            )

        self.assertEqual("ROLLED_BACK", result["candidate_status"])
        self.assertEqual("ROLLED_BACK", repo.rows[str(candidate["version_id"])]["status"])
        self.assertIsNone(repo.get_candidate_version(tenant_id="default"))

    def test_select_runtime_policy_version_uses_canary_for_low_risk_bucket(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.96}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_canary_allow_high_risk", False),
            patch.object(settings, "policy_shadow_enabled", False),
        ):
            selected, selector = select_runtime_policy_version(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                goal={"goal_id": "goal-low", "normalized_goal": "prepare brief", "risk_level": "low"},
                conversation_id="conv-low",
            )

        self.assertEqual("policy-canary", selected["version_id"])
        self.assertEqual("canary", selector["mode"])

    def test_select_runtime_policy_version_keeps_high_risk_goal_on_active(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.96}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_canary_allow_high_risk", False),
            patch.object(settings, "policy_shadow_enabled", False),
        ):
            selected, selector = select_runtime_policy_version(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                goal={"goal_id": "goal-high", "normalized_goal": "send ticket", "risk_level": "high"},
                conversation_id="conv-high",
            )

        self.assertEqual("policy-active", selected["version_id"])
        self.assertEqual("active", selector["mode"])
        self.assertEqual("high_risk_goal", selector["reason"])

    def test_select_runtime_policy_version_waits_for_shadow_probe_floor(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.96}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_shadow_enabled", True),
            patch.object(settings, "policy_shadow_min_probe_count", 2),
        ):
            selected, selector = select_runtime_policy_version(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                goal={"goal_id": "goal-shadow-floor", "normalized_goal": "prepare brief", "risk_level": "low"},
                conversation_id="conv-shadow-floor",
            )

        self.assertEqual("policy-active", selected["version_id"])
        self.assertEqual("shadow_probe_floor_not_met", selector["reason"])

    def test_select_runtime_policy_version_skips_canary_after_shadow_guardrail_failure(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.96}},
            comparison_payload={
                "last_eval_verdict": {"passed": True},
                "shadow_guardrail": {"ready": True, "passed": False},
            },
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_shadow_enabled", True),
        ):
            selected, selector = select_runtime_policy_version(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                goal={"goal_id": "goal-shadow-stop", "normalized_goal": "prepare brief", "risk_level": "low"},
                conversation_id="conv-shadow-stop",
            )

        self.assertEqual("policy-active", selected["version_id"])
        self.assertEqual("shadow_guardrail_failed", selector["reason"])

    def test_select_runtime_policy_version_skips_canary_after_shadow_portfolio_outcome_guardrail_failure(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.96}},
            comparison_payload={
                "last_eval_verdict": {"passed": True},
                "shadow_portfolio_outcome_guardrail": {"ready": True, "passed": False},
            },
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_shadow_enabled", True),
            patch.object(settings, "policy_shadow_min_probe_count", 0),
        ):
            selected, selector = select_runtime_policy_version(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                goal={
                    "goal_id": "goal-shadow-portfolio-outcome-stop",
                    "normalized_goal": "prepare brief",
                    "risk_level": "low",
                },
                conversation_id="conv-shadow-portfolio-outcome-stop",
            )

        self.assertEqual("policy-active", selected["version_id"])
        self.assertEqual("shadow_portfolio_outcome_guardrail_failed", selector["reason"])

    def test_select_runtime_policy_version_skips_canary_after_shadow_outcome_regret(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.96}},
            comparison_payload={
                "last_eval_verdict": {"passed": True},
                "shadow_outcome_counts": {"total": 2, "divergent_total": 2, "live_success_divergent": 2},
            },
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_shadow_enabled", True),
            patch.object(settings, "policy_shadow_min_probe_count", 0),
            patch.object(settings, "policy_shadow_max_regret_signal_rate", 0.4),
        ):
            selected, selector = select_runtime_policy_version(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                goal={"goal_id": "goal-shadow-regret", "normalized_goal": "prepare brief", "risk_level": "low"},
                conversation_id="conv-shadow-regret",
            )

        self.assertEqual("policy-active", selected["version_id"])
        self.assertEqual("shadow_outcome_regret_too_high", selector["reason"])

    def test_select_runtime_policy_version_skips_canary_after_shadow_outcome_guardrail_failure(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.96}},
            comparison_payload={
                "last_eval_verdict": {"passed": True},
                "shadow_outcome_guardrail": {"ready": True, "passed": False},
            },
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_shadow_enabled", True),
            patch.object(settings, "policy_shadow_min_probe_count", 0),
        ):
            selected, selector = select_runtime_policy_version(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                goal={"goal_id": "goal-shadow-outcome-stop", "normalized_goal": "prepare brief", "risk_level": "low"},
                conversation_id="conv-shadow-outcome-stop",
            )

        self.assertEqual("policy-active", selected["version_id"])
        self.assertEqual("shadow_outcome_guardrail_failed", selector["reason"])

    def test_select_runtime_policy_version_skips_canary_after_shadow_portfolio_guardrail_failure(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
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
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.96}},
            comparison_payload={
                "last_eval_verdict": {"passed": True},
                "shadow_portfolio_guardrail": {"ready": True, "passed": False},
            },
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_shadow_enabled", True),
            patch.object(settings, "policy_shadow_min_probe_count", 0),
        ):
            selected, selector = select_runtime_policy_version(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                goal={"goal_id": "goal-shadow-portfolio-stop", "normalized_goal": "prepare brief", "risk_level": "low"},
                conversation_id="conv-shadow-portfolio-stop",
            )

        self.assertEqual("policy-active", selected["version_id"])
        self.assertEqual("shadow_portfolio_guardrail_failed", selector["reason"])

    def test_select_runtime_policy_version_skips_canary_when_starvation_rate_too_high(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
            tenant_id="default",
            version_id="policy-active",
            version_tag="active",
            status="ACTIVE",
            base_version_id=None,
            source="bootstrap",
            memory_payload={
                "eval_summary": {
                    "success_rate": 0.95,
                    "portfolio_starvation_rate": 0.06,
                    "portfolio_subscription_timeout_rate": 0.08,
                    "portfolio_throughput_score": 0.74,
                }
            },
            comparison_payload={},
            created_by="user-1",
        )
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={
                "eval_summary": {
                    "success_rate": 0.96,
                    "portfolio_starvation_rate": 0.27,
                    "portfolio_subscription_timeout_rate": 0.08,
                    "portfolio_throughput_score": 0.79,
                }
            },
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_shadow_enabled", False),
        ):
            selected, selector = select_runtime_policy_version(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                goal={"goal_id": "goal-starved", "normalized_goal": "prepare brief", "risk_level": "low"},
                conversation_id="conv-starved",
            )

        self.assertEqual("policy-active", selected["version_id"])
        self.assertEqual("canary_starvation_rate_too_high", selector["reason"])

    def test_select_runtime_policy_version_skips_canary_when_subscription_timeout_rate_too_high(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
            tenant_id="default",
            version_id="policy-active",
            version_tag="active",
            status="ACTIVE",
            base_version_id=None,
            source="bootstrap",
            memory_payload={
                "eval_summary": {
                    "success_rate": 0.95,
                    "portfolio_starvation_rate": 0.04,
                    "portfolio_subscription_timeout_rate": 0.05,
                    "portfolio_throughput_score": 0.72,
                }
            },
            comparison_payload={},
            created_by="user-1",
        )
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={
                "eval_summary": {
                    "success_rate": 0.96,
                    "portfolio_starvation_rate": 0.04,
                    "portfolio_subscription_timeout_rate": 0.28,
                    "portfolio_throughput_score": 0.75,
                }
            },
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_shadow_enabled", False),
        ):
            selected, selector = select_runtime_policy_version(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                goal={"goal_id": "goal-timeout", "normalized_goal": "prepare brief", "risk_level": "low"},
                conversation_id="conv-timeout",
            )

        self.assertEqual("policy-active", selected["version_id"])
        self.assertEqual("canary_subscription_timeout_rate_too_high", selector["reason"])

    def test_select_runtime_policy_version_skips_canary_when_throughput_too_low(self) -> None:
        repo = _FakePolicyRepo()
        active = repo.create_policy_version(
            tenant_id="default",
            version_id="policy-active",
            version_tag="active",
            status="ACTIVE",
            base_version_id=None,
            source="bootstrap",
            memory_payload={
                "eval_summary": {
                    "success_rate": 0.95,
                    "portfolio_starvation_rate": 0.04,
                    "portfolio_subscription_timeout_rate": 0.07,
                    "portfolio_throughput_score": 0.77,
                }
            },
            comparison_payload={},
            created_by="user-1",
        )
        repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id=str(active["version_id"]),
            source="eval_feedback",
            memory_payload={
                "eval_summary": {
                    "success_rate": 0.96,
                    "portfolio_starvation_rate": 0.04,
                    "portfolio_subscription_timeout_rate": 0.07,
                    "portfolio_throughput_score": 0.52,
                }
            },
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by="user-1",
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_shadow_enabled", False),
        ):
            selected, selector = select_runtime_policy_version(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                goal={"goal_id": "goal-throughput", "normalized_goal": "prepare brief", "risk_level": "low"},
                conversation_id="conv-throughput",
            )

        self.assertEqual("policy-active", selected["version_id"])
        self.assertEqual("canary_portfolio_throughput_too_low", selector["reason"])

    def test_feedback_auto_evaluates_and_promotes_candidate_when_ready(self) -> None:
        repo = _FakePolicyRepo()
        with (
            patch.object(settings, "policy_auto_eval_enabled", True),
            patch.object(settings, "policy_auto_eval_promote", True),
            patch.object(settings, "policy_auto_eval_min_episode_feedback", 2),
            patch.object(settings, "policy_auto_eval_min_portfolio_feedback", 0),
            patch.object(settings, "policy_auto_eval_min_total_feedback", 2),
            patch.object(settings, "policy_auto_eval_feedback_delta", 2),
            patch.object(settings, "policy_shadow_enabled", False),
        ):
            first = record_episode_feedback(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                episode={
                    "chosen_strategy": "workflow_call",
                    "outcome_status": "SUCCEEDED",
                    "tool_names": [],
                    "useful_lessons": ["Durable continuation closed the loop cleanly."],
                    "episode_payload": {"outcome_signal": {"latest_result": {"status": "SUCCEEDED"}}},
                },
            )
            self.assertEqual("CANDIDATE", first["status"])
            second = record_episode_feedback(
                repo=repo,
                tenant_id="default",
                actor_user_id="user-1",
                episode={
                    "chosen_strategy": "workflow_call",
                    "outcome_status": "SUCCEEDED",
                    "tool_names": [],
                    "useful_lessons": ["Stable continuation improved throughput."],
                    "episode_payload": {"outcome_signal": {"latest_result": {"status": "SUCCEEDED"}}},
                },
            )

        self.assertEqual("ACTIVE", second["status"])
        self.assertEqual(1, len(repo.eval_rows))
        self.assertTrue(second["auto_eval_result"]["promoted"])
        comparison_payload = repo.rows[repo.active_id]["comparison_payload"]
        self.assertEqual(2, comparison_payload["last_auto_eval_feedback_counts"]["episodes"])
        self.assertTrue(comparison_payload["last_auto_eval_result"]["passed"])
