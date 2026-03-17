import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.deps import get_current_user
from app.main import app, conversation_repo, task_repo, turn_repo


class AssistantExperienceApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._init_pool_patcher = patch("app.main.init_pool", return_value=None)
        self._close_pool_patcher = patch("app.main.close_pool", return_value=None)
        self._ensure_schema_compat_patcher = patch("app.main.ensure_schema_compat", return_value=None)
        self._init_pool_patcher.start()
        self._close_pool_patcher.start()
        self._ensure_schema_compat_patcher.start()
        app.dependency_overrides[get_current_user] = lambda: {
            "id": "00000000-0000-0000-0000-000000000001",
            "tenant_id": "default",
            "role": "user",
        }
        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.pop(get_current_user, None)
        self._ensure_schema_compat_patcher.stop()
        self._close_pool_patcher.stop()
        self._init_pool_patcher.stop()

    def test_list_assistant_conversations(self) -> None:
        now = datetime.now(tz=timezone.utc)
        rows = [
            {
                "conversation_id": "conv-001",
                "tenant_id": "default",
                "user_id": "00000000-0000-0000-0000-000000000001",
                "message_history": [
                    {"role": "user", "message": "hello"},
                    {"role": "assistant", "message": "hi there", "route": "direct_answer"},
                ],
                "last_task_result": {},
                "last_tool_result": {},
                "user_preferences": {},
                "task_count": 2,
                "running_task_count": 1,
                "waiting_approval_count": 0,
                "created_at": now,
                "updated_at": now,
            }
        ]
        with patch.object(conversation_repo, "list_conversations_for_user", return_value=rows):
            resp = self.client.get("/assistant/conversations")
        self.assertEqual(200, resp.status_code)
        data = resp.json()
        self.assertEqual(1, len(data))
        self.assertEqual("conv-001", data[0]["conversation_id"])
        self.assertEqual("hello", data[0]["last_user_message"])
        self.assertEqual("hi there", data[0]["last_assistant_message"])
        self.assertEqual(1, data[0]["running_task_count"])

    def test_get_assistant_conversation_detail(self) -> None:
        now = datetime.now(tz=timezone.utc)
        conversation = {
            "conversation_id": "conv-002",
            "tenant_id": "default",
            "user_id": "00000000-0000-0000-0000-000000000001",
            "message_history": [
                {"role": "user", "message": "search docs"},
                {"role": "assistant", "message": "done", "route": "tool_task"},
            ],
            "last_task_result": {"task_id": "task-1"},
            "last_tool_result": {"tool_id": "web_search"},
            "user_preferences": {"response_style": "concise"},
            "created_at": now,
            "updated_at": now,
        }
        tasks = [
            {
                "id": "task-1",
                "task_type": "tool_flow",
                "status": "RUNNING",
                "trace_id": "trace-1",
                "input_masked": {"planner": {"action": "use_tool"}},
                "output_masked": {},
                "error_code": None,
                "error_message": None,
                "latest_step_key": "assistant_tool_run",
                "tool_call_count": 1,
                "waiting_approval_count": 0,
                "created_at": now,
                "updated_at": now,
            }
        ]
        with (
            patch.object(conversation_repo, "get_conversation", return_value=conversation),
            patch.object(task_repo, "list_assistant_tasks_for_conversation", return_value=tasks),
            patch.object(turn_repo, "list_turns_for_conversation", return_value=[]),
        ):
            resp = self.client.get("/assistant/conversations/conv-002")
        self.assertEqual(200, resp.status_code)
        data = resp.json()
        self.assertEqual("conv-002", data["conversation_id"])
        self.assertEqual(2, data["context_window"])
        self.assertEqual([], data["turn_history"])
        self.assertEqual(1, len(data["task_history"]))
        self.assertEqual("tool_task", data["task_history"][0]["route"])
        self.assertTrue(data["task_history"][0]["status_label"])

    def test_get_assistant_task_trace(self) -> None:
        now = datetime.now(tz=timezone.utc)
        task = {
            "id": "task-3",
            "tenant_id": "default",
            "task_type": "research_summary",
            "status": "WAITING_HUMAN",
            "created_by": "00000000-0000-0000-0000-000000000001",
            "input_masked": {
                "conversation_id": "conv-3",
                "planner": {"action": "start_workflow", "selected_tool": "web_search", "tool_candidates": ["web_search"]},
                "retrieval_hits": [{"title": "README", "snippet": "assistant"}],
            },
            "output_masked": {},
            "runtime_state": {
                "goal": {
                    "normalized_goal": "summarize assistant runtime",
                    "risk_level": "medium",
                    "success_criteria": ["produce grounded summary"],
                    "constraints": [],
                    "unknowns": [],
                    "user_intent": "research_summary",
                },
                "task_state": {
                    "current_goal": {
                        "normalized_goal": "summarize assistant runtime",
                        "risk_level": "medium",
                        "success_criteria": ["produce grounded summary"],
                        "constraints": [],
                        "unknowns": [],
                        "user_intent": "research_summary",
                    },
                    "current_subgoals": [],
                    "observations": [],
                    "pending_approvals": ["web_search"],
                    "latest_result": {},
                    "current_phase": "wait",
                    "available_actions": ["workflow_call", "wait"],
                    "fallback_state": "idle",
                },
                "current_action": {
                    "action_type": "workflow_call",
                    "target": "web_search",
                    "input": {},
                    "rationale": "handoff to durable workflow",
                    "expected_result": "The durable runtime advances the goal and emits a new observation or final result.",
                    "success_conditions": ["workflow_progress_observed", "runtime_state_advanced"],
                    "fallback": "respond",
                    "stop_conditions": ["workflow_failed_final", "workflow_cancelled", "budget_exhausted"],
                    "requires_approval": False,
                },
                "policy": {
                    "selected_action": "workflow_call",
                    "selection_reasons": ["open-ended goal"],
                    "fallback_action": "respond",
                    "replan_triggers": ["retryable_tool_failure"],
                    "approval_triggered": False,
                    "ask_user_triggered": False,
                    "episode_retrieval_triggered": False,
                    "similar_episode_ids": [],
                    "planner_action": "start_workflow",
                },
                "decision": {
                    "action": "workflow_call",
                    "route": "workflow_task",
                    "summary": "Policy selected workflow_call.",
                    "candidate_actions": [
                        {"action_type": "workflow_call", "disposition": "selected", "reason": "Selected by policy."},
                        {"action_type": "respond", "disposition": "deferred", "reason": "Need durable execution first."},
                    ],
                    "why_not": {"respond": "Need durable execution first."},
                },
                "steps": [
                    {
                        "key": "policy_action_selection",
                        "phase": "plan",
                        "title": "Choose next action",
                        "status": "completed",
                        "summary": "Policy selected workflow_call.",
                        "state_before": {"current_phase": "plan"},
                        "state_after": {"current_phase": "plan", "current_action": {"action_type": "workflow_call"}},
                    }
                ],
                "episodes": [],
            },
            "error_code": None,
            "error_message": None,
            "trace_id": "trace-3",
            "created_at": now,
            "updated_at": now,
        }
        runs = [{"id": "run-1", "run_no": 1, "status": "WAITING_HUMAN", "started_at": now, "ended_at": None}]
        steps = [
            {
                "id": 1,
                "run_id": "run-1",
                "step_key": "task_create",
                "status": "QUEUED",
                "payload_masked": {"task_type": "research_summary"},
                "created_at": now,
            }
        ]
        tool_calls = [
            {
                "tool_call_id": "call-1",
                "tool_id": "web_search",
                "status": "SUCCEEDED",
                "reason_code": None,
                "duration_ms": 32,
                "request_masked": {"query": "assistant"},
                "response_masked": {"results": [{"title": "README"}]},
                "created_at": now,
            }
        ]
        approvals = [
            {
                "id": "approval-1",
                "status": "WAITING_HUMAN",
                "reason": "need human review",
                "created_at": now,
                "updated_at": now,
            }
        ]
        with (
            patch.object(task_repo, "get_task_by_id", return_value=task),
            patch.object(task_repo, "list_runs_for_task", return_value=runs),
            patch.object(task_repo, "list_steps_for_run_ids", return_value=steps),
            patch.object(task_repo, "list_tool_calls_for_task", return_value=tool_calls),
            patch.object(task_repo, "list_approvals_for_task", return_value=approvals),
        ):
            resp = self.client.get("/assistant/tasks/task-3/trace")
        self.assertEqual(200, resp.status_code)
        data = resp.json()
        self.assertEqual("task-3", data["task"]["task_id"])
        self.assertEqual("等待审批", data["task"]["status_label"])
        self.assertEqual(1, len(data["retrieval_hits"]))
        self.assertEqual(1, len(data["tool_calls"]))
        self.assertEqual(1, len(data["approvals"]))
        self.assertEqual("summarize assistant runtime", data["goal"]["normalized_goal"])
        self.assertEqual("workflow_call", data["policy"]["selected_action"])
        self.assertEqual("respond", data["current_action"]["fallback"])
        self.assertEqual("Need durable execution first.", data["runtime_debugger"]["why_not"]["respond"])
        self.assertEqual(1, len(data["runtime_steps"]))
        self.assertIn("任务类型", data["task_summary"])


if __name__ == "__main__":
    unittest.main()
