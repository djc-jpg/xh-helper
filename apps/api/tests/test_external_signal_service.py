import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.config import settings
from app.main import app
from app.services.external_signal_service import dispatch_external_adapter_signal, dispatch_external_signal
from app.services.goal_runtime_service import sync_goal_progress


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


class _FakeTaskRepo:
    def __init__(self) -> None:
        self.audit_rows: list[dict] = []

    def insert_audit_log(
        self,
        *,
        tenant_id: str,
        actor_user_id: str | None,
        action: str,
        target_type: str,
        target_id: str,
        detail_masked: dict,
        trace_id: str,
    ) -> None:
        self.audit_rows.append(
            {
                "tenant_id": tenant_id,
                "actor_user_id": actor_user_id,
                "action": action,
                "target_type": target_type,
                "target_id": target_id,
                "detail_masked": dict(detail_masked),
                "trace_id": trace_id,
            }
        )


class ExternalSignalServiceTests(unittest.TestCase):
    def test_dispatch_external_signal_resumes_waiting_goal_and_audits(self) -> None:
        goal_repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=goal_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-vendor",
            goal={
                "normalized_goal": "wait for vendor callback",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "vendor-callback",
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
        task_repo = _FakeTaskRepo()

        result = dispatch_external_signal(
            goal_repo=goal_repo,
            task_repo=task_repo,
            tenant_id="default",
            worker_id="worker-local",
            signal={
                "signal_id": "signal-vendor-1",
                "source": "vendor_webhook",
                "event_key": "vendor-callback",
                "payload": {"job_id": "job-1"},
            },
            trace_id="trace-signal-1",
        )

        updated = goal_repo.get_goal(tenant_id="default", goal_id=str(waiting["goal_id"]))
        self.assertEqual(1, result["matched_goal_count"])
        self.assertEqual([str(waiting["goal_id"])], result["resumed_goal_ids"])
        self.assertEqual("ACTIVE", updated["status"])
        self.assertEqual("scheduler_cooldown", updated["goal_state"]["wake_condition"]["kind"])
        self.assertEqual("external_signal", updated["goal_state"]["task_state"]["latest_result"]["event_kind"])
        self.assertEqual("goal_external_signal_ingest", task_repo.audit_rows[0]["action"])

    def test_dispatch_external_signal_waits_for_remaining_composite_subscriptions(self) -> None:
        goal_repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=goal_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-composite",
            goal={
                "normalized_goal": "wait for vendor callback and artifact",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "vendor-callback",
                        "source": "vendor_webhook",
                        "resume_action": "workflow_call",
                    },
                    {
                        "kind": "external_signal",
                        "event_key": "artifact-ready",
                        "source": "artifact_store",
                        "resume_action": "workflow_call",
                    },
                ],
            },
            runtime_state={
                "status": "WAITING_HUMAN",
                "current_phase": "wait",
                "current_action": {"action_type": "wait"},
                "reflection": {"next_action": "workflow_call"},
            },
        )

        first = dispatch_external_signal(
            goal_repo=goal_repo,
            task_repo=_FakeTaskRepo(),
            tenant_id="default",
            worker_id="worker-local",
            signal={
                "source": "vendor_webhook",
                "event_key": "vendor-callback",
                "payload": {"status": "ready"},
            },
            trace_id="trace-signal-2",
        )
        partially_waiting = goal_repo.get_goal(tenant_id="default", goal_id=str(waiting["goal_id"]))
        self.assertEqual([], first["resumed_goal_ids"])
        self.assertEqual([str(waiting["goal_id"])], first["still_waiting_goal_ids"])
        self.assertEqual("WAITING", partially_waiting["status"])
        self.assertEqual(1, len(partially_waiting["goal_state"]["pending_event_subscriptions"]))

        second = dispatch_external_signal(
            goal_repo=goal_repo,
            task_repo=_FakeTaskRepo(),
            tenant_id="default",
            worker_id="worker-local",
            signal={
                "source": "artifact_store",
                "event_key": "artifact-ready",
                "payload": {"artifact_id": "artifact-7"},
            },
            trace_id="trace-signal-3",
        )
        resumed = goal_repo.get_goal(tenant_id="default", goal_id=str(waiting["goal_id"]))
        self.assertEqual([str(waiting["goal_id"])], second["resumed_goal_ids"])
        self.assertEqual("ACTIVE", resumed["status"])
        self.assertEqual(2, len(resumed["goal_state"]["event_memory"]))

    def test_dispatch_external_signal_supports_alias_event_keys(self) -> None:
        goal_repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=goal_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-alias",
            goal={
                "normalized_goal": "wait for aliased vendor signal",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "vendor-job-42",
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

        result = dispatch_external_signal(
            goal_repo=goal_repo,
            task_repo=_FakeTaskRepo(),
            tenant_id="default",
            worker_id="worker-local",
            signal={
                "source": "vendor_webhook",
                "event_key": "vendor-callback",
                "event_aliases": ["vendor-job-42"],
                "payload": {"job_id": "42"},
            },
            trace_id="trace-signal-4",
        )

        self.assertEqual([str(waiting["goal_id"])], result["resumed_goal_ids"])

    def test_dispatch_external_signal_derives_source_aliases_from_payload(self) -> None:
        goal_repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=goal_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-source-alias",
            goal={
                "normalized_goal": "wait for vendor job callback",
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

        result = dispatch_external_signal(
            goal_repo=goal_repo,
            task_repo=_FakeTaskRepo(),
            tenant_id="default",
            worker_id="worker-local",
            signal={
                "source": "vendor_webhook",
                "event_key": "vendor-callback",
                "payload": {"job_id": "job-42", "status": "completed"},
            },
            trace_id="trace-signal-5",
        )

        self.assertEqual([str(waiting["goal_id"])], result["resumed_goal_ids"])
        self.assertIn("vendor_webhook:job_id:job-42", result["event_keys"])
        self.assertIn("vendor_webhook:topic:completed", result["event_keys"])
        self.assertEqual("completed", result["event_topic"])
        self.assertEqual("completed", result["adapter"]["source_status"])
        self.assertEqual("vendor_webhook", result["adapter"]["source"])

    def test_dispatch_external_signal_supports_topic_and_entity_reference_aliases(self) -> None:
        goal_repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=goal_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-topic-alias",
            goal={
                "normalized_goal": "wait for artifact store ready topic",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "artifact_store:topic:artifact_ready",
                        "source": "artifact_store",
                        "resume_action": "workflow_call",
                    },
                    {
                        "kind": "external_signal",
                        "event_key": "artifact_store:ref:artifact-99",
                        "source": "artifact_store",
                        "resume_action": "workflow_call",
                    },
                ],
            },
            runtime_state={
                "status": "WAITING_HUMAN",
                "current_phase": "wait",
                "current_action": {"action_type": "wait"},
                "reflection": {"next_action": "workflow_call"},
            },
        )

        first = dispatch_external_signal(
            goal_repo=goal_repo,
            task_repo=_FakeTaskRepo(),
            tenant_id="default",
            worker_id="worker-local",
            signal={
                "source": "artifact_store",
                "event_key": "artifact-ready",
                "event_topic": "artifact_ready",
                "payload": {"artifact_id": "artifact-99"},
            },
            trace_id="trace-signal-6",
        )
        self.assertEqual([], first["still_waiting_goal_ids"])
        self.assertEqual([str(waiting["goal_id"])], first["resumed_goal_ids"])
        self.assertEqual("artifact_ready", first["event_topic"])
        self.assertIn("artifact-99", first["entity_refs"])
        self.assertEqual("artifact_store", first["adapter"]["source"])

    def test_dispatch_external_signal_failure_resumes_goal_into_replan(self) -> None:
        goal_repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=goal_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-vendor-failure",
            goal={
                "normalized_goal": "wait for vendor job result",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "vendor_webhook:job_id:job-77",
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

        result = dispatch_external_signal(
            goal_repo=goal_repo,
            task_repo=_FakeTaskRepo(),
            tenant_id="default",
            worker_id="worker-local",
            signal={
                "source": "vendor_webhook",
                "event_key": "vendor-callback",
                "payload": {"job_id": "job-77", "status": "failed"},
            },
            trace_id="trace-signal-6b",
        )

        updated = goal_repo.get_goal(tenant_id="default", goal_id=str(waiting["goal_id"]))
        self.assertEqual([str(waiting["goal_id"])], result["resumed_goal_ids"])
        self.assertEqual("failure", result["adapter"]["outcome"])
        self.assertTrue(result["adapter"]["requires_replan"])
        self.assertEqual("replan", updated["goal_state"]["current_action"]["action_type"])
        self.assertTrue(updated["goal_state"]["reflection"]["requires_replan"])

    def test_dispatch_external_signal_records_portfolio_wait_learning(self) -> None:
        goal_repo = _FakeGoalRepo()
        sync_goal_progress(
            repo=goal_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-learning",
            goal={
                "normalized_goal": "wait for vendor callback",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "vendor-callback",
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

        with (
            patch("app.services.external_signal_service.record_external_signal_feedback"),
            patch("app.services.external_signal_service.record_portfolio_feedback") as record_feedback,
        ):
            dispatch_external_signal(
                goal_repo=goal_repo,
                policy_repo=object(),
                task_repo=_FakeTaskRepo(),
                tenant_id="default",
                worker_id="worker-local",
                signal={
                    "signal_id": "signal-learning-1",
                    "source": "vendor_webhook",
                    "event_key": "vendor-callback",
                    "payload": {"job_id": "job-9"},
                },
                trace_id="trace-signal-learning",
            )

        self.assertTrue(
            any(
                kwargs.get("feedback", {}).get("event_kind") == "external_wait_success"
                for _, kwargs in record_feedback.call_args_list
            )
        )

    def test_dispatch_external_signal_filters_same_topic_by_entity_ref(self) -> None:
        goal_repo = _FakeGoalRepo()
        waiting_a = sync_goal_progress(
            repo=goal_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-entity-a",
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
        waiting_b = sync_goal_progress(
            repo=goal_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-entity-b",
            goal={
                "normalized_goal": "wait for artifact 99",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "artifact_store:topic:artifact_ready",
                        "source": "artifact_store",
                        "event_topic": "artifact_ready",
                        "entity_refs": ["artifact-99"],
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

        result = dispatch_external_signal(
            goal_repo=goal_repo,
            task_repo=_FakeTaskRepo(),
            tenant_id="default",
            worker_id="worker-local",
            signal={
                "source": "artifact_store",
                "event_key": "artifact-ready",
                "event_topic": "artifact_ready",
                "payload": {"artifact_id": "artifact-99"},
            },
            trace_id="trace-signal-7",
        )

        updated_a = goal_repo.get_goal(tenant_id="default", goal_id=str(waiting_a["goal_id"]))
        updated_b = goal_repo.get_goal(tenant_id="default", goal_id=str(waiting_b["goal_id"]))
        self.assertEqual([str(waiting_b["goal_id"])], result["resumed_goal_ids"])
        self.assertEqual([str(waiting_a["goal_id"])], result["still_waiting_goal_ids"])
        self.assertEqual("WAITING", updated_a["status"])
        self.assertEqual("ACTIVE", updated_b["status"])

    def test_dispatch_external_signal_derives_file_watch_adapter_metadata(self) -> None:
        goal_repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=goal_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-file-watch",
            goal={
                "normalized_goal": "wait for generated report file",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "file_watch:ref:reports/daily.md",
                        "source": "file_watch",
                        "entity_refs": ["reports/daily.md"],
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

        task_repo = _FakeTaskRepo()
        result = dispatch_external_signal(
            goal_repo=goal_repo,
            task_repo=task_repo,
            tenant_id="default",
            worker_id="worker-local",
            signal={
                "source": "file_watch",
                "event_key": "fs-change",
                "payload": {"path": "reports/daily.md", "change_type": "modified"},
            },
            trace_id="trace-signal-file",
        )

        self.assertEqual([str(waiting["goal_id"])], result["resumed_goal_ids"])
        self.assertEqual("modified", result["event_topic"])
        self.assertIn("reports/daily.md", result["entity_refs"])
        self.assertEqual("modified", result["adapter"]["source_operation"])
        self.assertEqual("file_watch", task_repo.audit_rows[0]["detail_masked"]["adapter"]["source"])

    def test_dispatch_external_adapter_signal_derives_primary_event_key_from_source_payload(self) -> None:
        goal_repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=goal_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-adapter",
            goal={
                "normalized_goal": "wait for artifact adapter callback",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "artifact_store:topic:artifact_ready",
                        "source": "artifact_store",
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

        result = dispatch_external_adapter_signal(
            goal_repo=goal_repo,
            task_repo=_FakeTaskRepo(),
            tenant_id="default",
            worker_id="worker-local",
            source="artifact-store",
            signal={
                "event_topic": "artifact_ready",
                "payload": {"artifact_id": "artifact-123", "operation": "uploaded"},
            },
            trace_id="trace-adapter-1",
        )

        self.assertEqual("artifact_store:topic:artifact_ready", result["event_key"])
        self.assertEqual([str(waiting["goal_id"])], result["resumed_goal_ids"])
        self.assertIn("artifact_store:ref:artifact-123", result["event_keys"])

    def test_dispatch_external_adapter_signal_supports_github_check_source(self) -> None:
        goal_repo = _FakeGoalRepo()
        waiting = sync_goal_progress(
            repo=goal_repo,
            tenant_id="default",
            user_id="user-1",
            conversation_id="conv-github-check",
            goal={
                "normalized_goal": "wait for github check result",
                "wake_requirements": [
                    {
                        "kind": "external_signal",
                        "event_key": "github_check:topic:success",
                        "source": "github_check",
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

        result = dispatch_external_adapter_signal(
            goal_repo=goal_repo,
            task_repo=_FakeTaskRepo(),
            tenant_id="default",
            worker_id="worker-local",
            source="github-check",
            signal={
                "payload": {
                    "check_run_id": "run-77",
                    "conclusion": "success",
                    "repository": "org/repo",
                }
            },
            trace_id="trace-adapter-github-1",
        )

        self.assertEqual("github_check:topic:success", result["event_key"])
        self.assertEqual([str(waiting["goal_id"])], result["resumed_goal_ids"])
        self.assertIn("github_check:ref:run-77", result["event_keys"])
        self.assertEqual("github_check", result["adapter"]["source"])

    def test_dispatch_external_adapter_signal_rejects_unknown_source(self) -> None:
        with self.assertRaises(ValueError):
            dispatch_external_adapter_signal(
                goal_repo=_FakeGoalRepo(),
                task_repo=_FakeTaskRepo(),
                tenant_id="default",
                worker_id="worker-local",
                source="unknown-adapter",
                signal={"payload": {"resource_id": "abc"}},
                trace_id="trace-adapter-2",
            )


class InternalExternalSignalApiTests(unittest.TestCase):
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

    def test_internal_goal_external_signal_dispatches_service(self) -> None:
        with patch(
            "app.main.dispatch_external_signal",
            return_value={"status": "ok", "matched_goal_count": 1, "resumed_goal_ids": ["goal-1"], "still_waiting_goal_ids": []},
        ) as dispatch_signal:
            resp = self.client.post(
                "/internal/goals/external-signal",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "signal_id": "signal-api-1",
                    "source": "vendor_webhook",
                    "event_key": "vendor-callback",
                    "payload": {"job_id": "1"},
                },
            )

        self.assertEqual(200, resp.status_code)
        self.assertEqual("ok", resp.json()["status"])
        dispatch_signal.assert_called_once()
        self.assertEqual("default", dispatch_signal.call_args.kwargs["tenant_id"])
        self.assertEqual(settings.default_worker_id, dispatch_signal.call_args.kwargs["worker_id"])

    def test_internal_goal_external_signal_requires_internal_auth(self) -> None:
        resp = self.client.post(
            "/internal/goals/external-signal",
            json={
                "tenant_id": "default",
                "source": "vendor_webhook",
                "event_key": "vendor-callback",
            },
        )
        self.assertEqual(401, resp.status_code)

    def test_internal_goal_external_adapter_signal_dispatches_service(self) -> None:
        with patch(
            "app.main.dispatch_external_adapter_signal",
            return_value={"status": "ok", "matched_goal_count": 1, "resumed_goal_ids": ["goal-1"], "still_waiting_goal_ids": []},
        ) as dispatch_signal:
            resp = self.client.post(
                "/internal/goals/external-signal/vendor-webhook",
                headers=self.headers,
                json={
                    "tenant_id": "default",
                    "signal_id": "signal-api-adapter-1",
                    "payload": {"job_id": "1", "status": "completed"},
                },
            )

        self.assertEqual(200, resp.status_code)
        self.assertEqual("ok", resp.json()["status"])
        dispatch_signal.assert_called_once()
        self.assertEqual("vendor-webhook", dispatch_signal.call_args.kwargs["source"])
        self.assertEqual("default", dispatch_signal.call_args.kwargs["tenant_id"])

    def test_internal_goal_external_adapter_signal_rejects_unknown_source(self) -> None:
        resp = self.client.post(
            "/internal/goals/external-signal/unknown-adapter",
            headers=self.headers,
            json={
                "tenant_id": "default",
                "payload": {"resource_id": "abc"},
            },
        )
        self.assertEqual(400, resp.status_code)
