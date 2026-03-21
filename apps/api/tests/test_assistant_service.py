import unittest
from unittest.mock import AsyncMock, patch

from app.config import settings
from app.schemas import AssistantChatRequest
from app.services.assistant_orchestration_service import (
    _capability_overview_response,
    _fallback_response_with_retrieval,
)
from app.services.assistant_service import assistant_chat


class _FakeConversationRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}

    def get_or_create_conversation(self, *, tenant_id: str, user_id: str, conversation_id: str):
        row = self.rows.get(conversation_id)
        if row:
            return row
        row = {
            "conversation_id": conversation_id,
            "tenant_id": tenant_id,
            "user_id": user_id,
            "message_history": [],
            "last_task_result": {},
            "last_tool_result": {},
            "user_preferences": {},
        }
        self.rows[conversation_id] = row
        return row

    def append_message(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str,
        role: str,
        message: str,
        route: str,
        metadata: dict | None,
        created_at: str,
        max_messages: int,
    ):
        del tenant_id, user_id, created_at
        row = self.rows[conversation_id]
        history = list(row.get("message_history") or [])
        item = {"role": role, "message": message, "route": route}
        if metadata:
            item["metadata"] = metadata
        history.append(item)
        row["message_history"] = history[-max_messages:]
        return row["message_history"]

    def update_memory(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str,
        last_task_result: dict | None = None,
        last_tool_result: dict | None = None,
        user_preferences: dict | None = None,
    ):
        del tenant_id, user_id
        row = self.rows[conversation_id]
        if last_task_result is not None:
            row["last_task_result"] = dict(last_task_result)
        if last_tool_result is not None:
            row["last_tool_result"] = dict(last_tool_result)
        if user_preferences is not None:
            row["user_preferences"] = dict(user_preferences)
        return {
            "last_task_result": row["last_task_result"],
            "last_tool_result": row["last_tool_result"],
            "user_preferences": row["user_preferences"],
        }


class _FakeTaskRepo:
    def __init__(self) -> None:
        self.created_tasks = 0
        self.created_runs = 0
        self.audit_logs = 0
        self.last_create_task_kwargs: dict | None = None
        self.conversation_tasks: list[dict] = []

    def create_task(self, **kwargs):
        self.created_tasks += 1
        self.last_create_task_kwargs = dict(kwargs)
        return {"id": f"task-{self.created_tasks}", "trace_id": kwargs["trace_id"], "budget": kwargs["budget"]}

    def create_run(self, **kwargs):
        self.created_runs += 1
        return {"id": f"run-{self.created_runs}"}

    def update_task_status(self, tenant_id: str, task_id: str, status_text: str) -> None:
        del tenant_id, task_id, status_text

    def append_step(self, **kwargs) -> bool:
        del kwargs
        return True

    def update_run_status(self, tenant_id: str, run_id: str, status_text: str) -> None:
        del tenant_id, run_id, status_text

    def mark_task_succeeded(self, tenant_id: str, task_id: str, payload_masked: dict) -> None:
        del tenant_id, task_id, payload_masked

    def mark_task_failed(
        self,
        tenant_id: str,
        task_id: str,
        status_text: str,
        error_code: str | None,
        error_message=None,
    ) -> None:
        del tenant_id, task_id, status_text, error_code, error_message

    def insert_audit_log(self, **kwargs) -> None:
        del kwargs
        self.audit_logs += 1

    def list_assistant_tasks_for_conversation(self, *, tenant_id: str, user_id: str, conversation_id: str, limit: int = 30):
        del tenant_id, user_id
        rows = [row for row in self.conversation_tasks if str(row.get("conversation_id") or "") == conversation_id]
        return rows[:limit]


class _FakeTurnRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}

    def create_turn(self, **kwargs):
        row = dict(kwargs)
        row["created_at"] = "2026-03-11T00:00:00+00:00"
        row["updated_at"] = "2026-03-11T00:00:00+00:00"
        self.rows[row["turn_id"]] = row
        return row


class _FakeEpisodeRepo:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def list_recent_episodes_for_user(self, *, tenant_id: str, user_id: str, limit: int = 30):
        del tenant_id, user_id
        return list(self.rows[:limit])

    def upsert_episode(
        self,
        *,
        tenant_id: str,
        user_id: str,
        conversation_id: str | None,
        turn_id: str | None,
        task_id: str | None,
        episode: dict,
    ):
        del tenant_id, user_id, conversation_id, turn_id, task_id
        self.rows = [row for row in self.rows if row.get("episode_id") != episode.get("episode_id")]
        self.rows.insert(0, dict(episode))
        return dict(episode)


class _FakeGateway:
    def __init__(self) -> None:
        self.calls = 0

    async def execute(self, req: dict):
        del req
        self.calls += 1
        return {
            "status": "SUCCEEDED",
            "tool_call_id": "tool-call-1",
            "reason_code": None,
            "result": {
                "results": [
                    {"title": "asyncio docs", "url": "https://docs.python.org/3/library/asyncio.html", "snippet": "..."}
                ]
            },
            "idempotent_hit": False,
        }


class _FakeToolRepo:
    def __init__(self) -> None:
        self.tools = [
            {
                "tool_name": "web_search",
                "version": "v1",
                "description": "Search docs",
                "input_schema": {"type": "object"},
                "risk_level": "low",
                "requires_approval": False,
                "supported_use_cases": ["knowledge_lookup", "docs_search"],
                "enabled": True,
            },
            {
                "tool_name": "email_ticketing",
                "version": "v1",
                "description": "Send tickets",
                "input_schema": {"type": "object"},
                "risk_level": "high",
                "requires_approval": True,
                "supported_use_cases": ["ticket_action"],
                "enabled": True,
            },
        ]

    def list_assistant_registry(self, *, tenant_id: str, enabled_only: bool = True, use_case: str | None = None):
        del tenant_id
        rows = list(self.tools)
        if enabled_only:
            rows = [x for x in rows if bool(x.get("enabled"))]
        if use_case:
            rows = [x for x in rows if use_case in list(x.get("supported_use_cases") or [])]
        return rows


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
        row["continuation_count"] = int(row.get("continuation_count") or 0)
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
            wake = dict(dict(row.get("goal_state") or {}).get("wake_condition") or {})
            if str(wake.get("kind") or "") != event_kind or str(wake.get("event_key") or "") != event_key:
                continue
            if user_id and str(row.get("user_id") or "") != user_id:
                continue
            if conversation_id is not None and str(row.get("conversation_id") or "") != str(conversation_id):
                continue
            rows.append(row)
        return rows


class _FakePolicyRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.active_id: str | None = None
        self.canary_id: str | None = None

    def get_active_version(self, *, tenant_id: str):
        del tenant_id
        return self.rows.get(self.active_id or "")

    def get_candidate_version(self, *, tenant_id: str):
        del tenant_id
        return self.rows.get(self.canary_id or "")

    def get_policy_version(self, *, tenant_id: str, version_id: str):
        del tenant_id
        return self.rows.get(version_id)

    def create_policy_version(self, **kwargs):
        row = dict(kwargs)
        row["memory_payload"] = dict(kwargs["memory_payload"])
        row["comparison_payload"] = dict(kwargs["comparison_payload"])
        self.rows[row["version_id"]] = row
        if row["status"] == "ACTIVE":
            self.active_id = row["version_id"]
        if row["status"] == "CANARY":
            self.canary_id = row["version_id"]
        return row

    def update_policy_version(self, *, tenant_id: str, version_id: str, memory_payload: dict, comparison_payload: dict):
        del tenant_id
        self.rows[version_id]["memory_payload"] = dict(memory_payload)
        self.rows[version_id]["comparison_payload"] = dict(comparison_payload)


class AssistantServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.conversation_repo = _FakeConversationRepo()
        self.episode_repo = _FakeEpisodeRepo()
        self.turn_repo = _FakeTurnRepo()
        self.task_repo = _FakeTaskRepo()
        self.gateway = _FakeGateway()
        self.tool_repo = _FakeToolRepo()
        self.goal_repo = _FakeGoalRepo()
        self.user = {"id": "00000000-0000-0000-0000-000000000001", "tenant_id": "default", "role": "user"}
        self.start_workflow = AsyncMock()
        self.qwen_enabled_patcher = patch("app.services.assistant_orchestration_service.qwen_client.is_enabled", return_value=False)
        self.qwen_enabled_patcher.start()

    async def asyncTearDown(self) -> None:
        self.qwen_enabled_patcher.stop()

    async def test_direct_answer_path(self) -> None:
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-direct",
            message="What can you do?",
            mode="direct_answer",
            metadata={},
        )
        result = await assistant_chat(
            conversation_repo=self.conversation_repo,
            episode_repo=self.episode_repo,
            turn_repo=self.turn_repo,
            task_repo=self.task_repo,
            tool_repo=self.tool_repo,
            gateway=self.gateway,
            req=req,
            tenant_id="default",
            user=self.user,
            trace_id="trace-direct",
            start_workflow=self.start_workflow,
        )
        self.assertEqual("direct_answer", result["response_type"])
        self.assertEqual("direct_answer", result["route"])
        self.assertEqual("answer_only", result["planner"]["action"])
        self.assertIsNone(result["task"])
        self.assertIsNotNone(result["turn"])
        self.assertEqual("SUCCEEDED", result["turn"]["status"])
        self.assertEqual(0, self.gateway.calls)
        self.assertEqual(0, self.task_repo.created_tasks)

    async def test_direct_answer_uses_qwen_when_available(self) -> None:
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-qwen",
            message="How does the workflow work?",
            mode="direct_answer",
            metadata={},
        )
        qwen_chat = AsyncMock(return_value="Qwen says: the workflow is durable.")
        with (
            patch("app.services.assistant_orchestration_service.qwen_client.is_enabled", return_value=True),
            patch("app.services.assistant_orchestration_service.qwen_client.chat_text", new=qwen_chat),
        ):
            result = await assistant_chat(
                conversation_repo=self.conversation_repo,
                episode_repo=self.episode_repo,
                turn_repo=self.turn_repo,
                task_repo=self.task_repo,
                tool_repo=self.tool_repo,
                gateway=self.gateway,
                req=req,
                tenant_id="default",
                user=self.user,
                trace_id="trace-qwen",
                start_workflow=self.start_workflow,
            )
        self.assertEqual("Qwen says: the workflow is durable.", result["message"])
        self.assertEqual("direct_answer", result["response_type"])
        self.assertEqual(20.0, qwen_chat.await_args.kwargs["timeout_s"])

    async def test_chinese_direct_answer_uses_shorter_qwen_timeout(self) -> None:
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-qwen-cn",
            message="\u8fd9\u4e2a workflow runtime \u662f\u600e\u4e48\u5de5\u4f5c\u7684\uff1f",
            mode="direct_answer",
            metadata={},
        )
        qwen_chat = AsyncMock(return_value="\u5b83\u4e3b\u8981\u8d1f\u8d23\u6301\u7eed\u6267\u884c\u548c\u72b6\u6001\u7f16\u6392\u3002")
        with (
            patch("app.services.assistant_orchestration_service.qwen_client.is_enabled", return_value=True),
            patch("app.services.assistant_orchestration_service.qwen_client.chat_text", new=qwen_chat),
        ):
            result = await assistant_chat(
                conversation_repo=self.conversation_repo,
                episode_repo=self.episode_repo,
                turn_repo=self.turn_repo,
                task_repo=self.task_repo,
                tool_repo=self.tool_repo,
                gateway=self.gateway,
                req=req,
                tenant_id="default",
                user=self.user,
                trace_id="trace-qwen-cn",
                start_workflow=self.start_workflow,
            )
        self.assertEqual("它主要负责持续执行和状态编排。", result["message"])
        self.assertEqual(12.0, qwen_chat.await_args.kwargs["timeout_s"])

    def test_acknowledgement_fallback_ignores_retrieval_noise(self) -> None:
        message = "Please reply in one short sentence to confirm you received this browser test message."
        retrieval_hits = [
            {
                "title": "product",
                "snippet": "ble workflow execution. - LangGraph for stateful graph planning.",
            }
        ]

        result = _fallback_response_with_retrieval(message, retrieval_hits, memory={})

        self.assertEqual("I received your message and can reply normally.", result)

    def test_english_retrieval_fallback_stays_in_english(self) -> None:
        result = _fallback_response_with_retrieval(
            "How does the workflow runtime work in this repo?",
            [{"title": "product", "snippet": "Temporal coordinates durable execution."}],
            memory={},
        )

        self.assertIn("I'll answer from the current workspace context first.", result)
        self.assertIn("Reference: product", result)

    def test_chinese_retrieval_fallback_reads_like_a_direct_answer(self) -> None:
        result = _fallback_response_with_retrieval(
            "\u8fd9\u4e2a workflow runtime \u662f\u600e\u4e48\u5de5\u4f5c\u7684\uff1f",
            [{"title": "runtime docs", "snippet": "Temporal \u8d1f\u8d23\u6301\u4e45\u5316\u6267\u884c\uff0cLangGraph \u8d1f\u8d23\u6709\u72b6\u6001\u89c4\u5212\u3002"}],
            memory={},
        )

        self.assertIn("\u76f4\u63a5\u7ed3\u8bba", result)
        self.assertIn("Temporal", result)

    def test_chinese_retrieval_fallback_does_not_dump_raw_english_snippet(self) -> None:
        result = _fallback_response_with_retrieval(
            "\u8fd9\u4e2a workflow runtime \u662f\u600e\u4e48\u5de5\u4f5c\u7684\uff1f",
            [{"title": "product", "snippet": "Supports multi-agent orchestration with Temporal, LangGraph, and Tool Gateway."}],
            memory={},
        )

        self.assertIn("Temporal", result)
        self.assertIn("LangGraph", result)
        self.assertIn("Tool Gateway", result)
        self.assertNotIn("Supports multi-agent orchestration", result)

    def test_capability_overview_matches_workspace_phrasing(self) -> None:
        result = _capability_overview_response("Please tell me what you can do in this workspace.")

        self.assertIsNotNone(result)
        self.assertIn("coding", result)

    def test_optimization_request_returns_ranked_project_advice(self) -> None:
        result = _fallback_response_with_retrieval(
            "\u57fa\u4e8e\u5f53\u524d\u9879\u76ee\u72b6\u6001\uff0c\u7ed9\u6211\u4e00\u4e2a\u53ef\u843d\u5730\u7684\u4f18\u5316\u65b9\u6848\uff0c\u4f18\u5148\u6309\u6536\u76ca\u6392\u5e8f\u3002",
            [],
            memory={},
        )

        self.assertIn("1.", result)
        self.assertIn("2.", result)
        self.assertIn("\u9ad8\u9891\u4e3b\u8def\u5f84", result)

    def test_repo_module_question_returns_concrete_module_map(self) -> None:
        result = _fallback_response_with_retrieval(
            "\u5e2e\u6211\u5b9a\u4f4d\u8fd9\u4e2a\u4ed3\u5e93\u91cc\u6700\u503c\u5f97\u5148\u770b\u7684\u5173\u952e\u6a21\u5757\uff0c\u5e76\u89e3\u91ca\u5b83\u4eec\u4e4b\u95f4\u7684\u5173\u7cfb\u3002",
            [],
            memory={},
        )

        self.assertIn("apps/api", result)
        self.assertIn("apps/worker", result)
        self.assertIn("runtime_backbone", result)

    async def test_progress_followup_prefers_latest_task_status(self) -> None:
        self.task_repo.conversation_tasks = [
            {
                "id": "task-progress-1",
                "conversation_id": "conv-progress",
                "task_type": "research_summary",
                "status": "SUCCEEDED",
                "latest_step_key": "workflow_start",
                "result_preview": "\u4efb\u52a1\u5df2\u7ecf\u5b8c\u6210\u5f53\u524d\u8fd9\u4e00\u8f6e\u5904\u7406\uff0c\u4f60\u53ef\u4ee5\u7ee7\u7eed\u8ffd\u95ee\uff0c\u6216\u53d1\u8d77\u4e0b\u4e00\u6b65\u3002",
            }
        ]
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-progress",
            message="\u73b0\u5728\u8fdb\u5c55\u5230\u54ea\u4e00\u6b65\u4e86\uff1f",
            mode="auto",
            metadata={},
        )
        result = await assistant_chat(
            conversation_repo=self.conversation_repo,
            episode_repo=self.episode_repo,
            turn_repo=self.turn_repo,
            task_repo=self.task_repo,
            tool_repo=self.tool_repo,
            gateway=self.gateway,
            req=req,
            tenant_id="default",
            user=self.user,
            trace_id="trace-progress-followup",
            start_workflow=self.start_workflow,
        )

        self.assertEqual("direct_answer", result["route"])
        self.assertIn("\u5df2\u7ecf\u5b8c\u6210", result["message"])
        self.assertIn("workflow_start", result["message"])
        self.assertIn("\u6700\u65b0\u7ed3\u679c", result["message"])

    async def test_tool_task_fast_path(self) -> None:
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-tool",
            message="search temporal workflow docs.python.org",
            mode="tool_task",
            metadata={},
        )
        result = await assistant_chat(
            conversation_repo=self.conversation_repo,
            episode_repo=self.episode_repo,
            turn_repo=self.turn_repo,
            task_repo=self.task_repo,
            tool_repo=self.tool_repo,
            gateway=self.gateway,
            req=req,
            tenant_id="default",
            user=self.user,
            trace_id="trace-tool",
            start_workflow=self.start_workflow,
        )
        self.assertEqual("tool_task", result["route"])
        self.assertEqual("direct_answer", result["response_type"])
        self.assertEqual("tool_task", result["turn"]["route"])
        self.assertEqual(1, self.gateway.calls)
        self.assertEqual("SUCCEEDED", result["task"]["status"])
        self.assertTrue(result["tool_result"]["results"])
        self.assertEqual("use_tool", result["planner"]["action"])
        self.assertEqual("respond", result["turn"]["agent_run"]["current_action"]["action_type"])
        self.assertEqual("respond", result["turn"]["agent_run"]["policy"]["selected_action"])
        self.assertTrue(result["turn"]["agent_run"]["current_action"]["expected_result"])
        self.assertEqual("respond", result["turn"]["agent_run"]["current_action"]["fallback"])

    async def test_high_risk_tool_requires_confirmation(self) -> None:
        self.tool_repo.tools = [
            {
                "tool_name": "email_ticketing",
                "version": "v1",
                "description": "Send tickets",
                "input_schema": {"type": "object"},
                "risk_level": "high",
                "requires_approval": True,
                "supported_use_cases": ["ticket_action"],
                "enabled": True,
            }
        ]
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-confirm",
            message="send ticket to oncall team",
            mode="tool_task",
            metadata={},
        )
        result = await assistant_chat(
            conversation_repo=self.conversation_repo,
            episode_repo=self.episode_repo,
            turn_repo=self.turn_repo,
            task_repo=self.task_repo,
            tool_repo=self.tool_repo,
            gateway=self.gateway,
            req=req,
            tenant_id="default",
            user=self.user,
            trace_id="trace-confirm",
            start_workflow=self.start_workflow,
        )
        self.assertTrue(result["need_confirmation"])
        self.assertIsNone(result["task"])
        self.assertEqual("need_approval", result["planner"]["action"])
        self.assertEqual("wait", result["turn"]["agent_run"]["current_action"]["action_type"])
        self.assertTrue(result["turn"]["agent_run"]["policy"]["approval_triggered"])
        self.assertIn("\u9ad8\u98ce\u9669\u64cd\u4f5c", result["message"])
        self.assertIn("\u64cd\u4f5c\u5458", result["message"])
        self.assertNotIn("email_ticketing", result["message"])
        self.assertNotIn("\u4f60\u786e\u8ba4\u540e", result["message"])

    async def test_confirmed_high_risk_tool_routes_to_workflow(self) -> None:
        self.tool_repo.tools = [
            {
                "tool_name": "email_ticketing",
                "version": "v1",
                "description": "Send tickets",
                "input_schema": {"type": "object"},
                "risk_level": "high",
                "requires_approval": True,
                "supported_use_cases": ["ticket_action"],
                "enabled": True,
            }
        ]
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-confirmed-risk",
            message="send ticket to oncall team",
            mode="tool_task",
            metadata={"confirmed": True},
        )
        with patch(
            "app.services.assistant_orchestration_service.service_create_task",
            AsyncMock(return_value={"task_id": "task-approval", "run_id": "run-approval", "status": "QUEUED", "idempotent": False}),
        ):
            result = await assistant_chat(
                conversation_repo=self.conversation_repo,
                episode_repo=self.episode_repo,
                turn_repo=self.turn_repo,
                task_repo=self.task_repo,
                tool_repo=self.tool_repo,
                gateway=self.gateway,
                req=req,
                tenant_id="default",
                user=self.user,
                trace_id="trace-confirmed-risk",
                start_workflow=self.start_workflow,
            )
        self.assertEqual("workflow_task", result["route"])
        self.assertEqual("task_created", result["response_type"])
        self.assertEqual("task-approval", result["task"]["task_id"])

    async def test_workflow_task_path(self) -> None:
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-workflow",
            message="Please prepare a research summary report",
            mode="workflow_task",
            metadata={},
        )
        with patch(
            "app.services.assistant_orchestration_service.service_create_task",
            AsyncMock(return_value={"task_id": "task-wf", "run_id": "run-wf", "status": "QUEUED", "idempotent": False}),
        ) as mocked_create_task:
            result = await assistant_chat(
                conversation_repo=self.conversation_repo,
                episode_repo=self.episode_repo,
                turn_repo=self.turn_repo,
                task_repo=self.task_repo,
                tool_repo=self.tool_repo,
                gateway=self.gateway,
                req=req,
                tenant_id="default",
                user=self.user,
                trace_id="trace-workflow",
                start_workflow=self.start_workflow,
            )
        self.assertEqual("workflow_task", result["route"])
        self.assertEqual("task_created", result["response_type"])
        self.assertEqual("task-wf", result["task"]["task_id"])
        self.assertEqual(1, mocked_create_task.await_count)
        workflow_req = mocked_create_task.await_args.kwargs["req"]
        self.assertIn("runtime_state", workflow_req.input)
        self.assertEqual("workflow_call", workflow_req.input["runtime_state"]["current_action"]["action_type"])
        self.assertEqual("workflow_task", workflow_req.input["runtime_state"]["route"])
        self.assertTrue(workflow_req.input["runtime_state"]["steps"])
        self.assertTrue(workflow_req.input["runtime_state"]["decision"]["candidate_actions"])

    async def test_multi_turn_memory_uses_last_tool_result(self) -> None:
        first = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-memory",
            message="search temporal workflow docs.python.org",
            mode="tool_task",
            metadata={"confirmed": True},
        )
        await assistant_chat(
            conversation_repo=self.conversation_repo,
            episode_repo=self.episode_repo,
            turn_repo=self.turn_repo,
            task_repo=self.task_repo,
            tool_repo=self.tool_repo,
            gateway=self.gateway,
            req=first,
            tenant_id="default",
            user=self.user,
            trace_id="trace-memory-1",
            start_workflow=self.start_workflow,
        )

        second = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-memory",
            message="what was my last tool result?",
            mode="direct_answer",
            metadata={},
        )
        result = await assistant_chat(
            conversation_repo=self.conversation_repo,
            episode_repo=self.episode_repo,
            turn_repo=self.turn_repo,
            task_repo=self.task_repo,
            tool_repo=self.tool_repo,
            gateway=self.gateway,
            req=second,
            tenant_id="default",
            user=self.user,
            trace_id="trace-memory-2",
            start_workflow=self.start_workflow,
        )
        self.assertTrue(
            "上一轮工具执行的结果摘要" in result["message"]
            or "asyncio docs" in result["message"]
            or "web_search" in result["message"]
        )

    async def test_ambiguous_request_triggers_ask_user(self) -> None:
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-ask",
            message="help with it",
            mode="auto",
            metadata={},
        )
        result = await assistant_chat(
            conversation_repo=self.conversation_repo,
            episode_repo=self.episode_repo,
            turn_repo=self.turn_repo,
            task_repo=self.task_repo,
            tool_repo=self.tool_repo,
            gateway=self.gateway,
            req=req,
            tenant_id="default",
            user=self.user,
            trace_id="trace-ask",
            start_workflow=self.start_workflow,
        )
        self.assertEqual("direct_answer", result["route"])
        self.assertIn("关键信息", result["message"])
        self.assertEqual("ask_user", result["turn"]["agent_run"]["current_action"]["action_type"])

    async def test_episode_biases_route_to_workflow(self) -> None:
        self.episode_repo.rows = [
            {
                "episode_id": "episode-prev",
                "normalized_goal": "prepare a research summary report",
                "task_summary": "prepare a research summary report",
                "chosen_strategy": "workflow_call",
                "action_types": ["workflow_call"],
                "tool_names": ["web_search"],
                "outcome_status": "SUCCEEDED",
                "final_outcome": "Long workflow succeeded",
                "useful_lessons": ["Durable workflow handoff works better for multi-step open goals."],
            }
        ]
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-episode",
            message="prepare a research summary report",
            mode="auto",
            metadata={},
        )
        with patch(
            "app.services.assistant_orchestration_service.service_create_task",
            AsyncMock(return_value={"task_id": "task-episode", "run_id": "run-episode", "status": "QUEUED", "idempotent": False}),
        ):
            result = await assistant_chat(
                conversation_repo=self.conversation_repo,
                episode_repo=self.episode_repo,
                turn_repo=self.turn_repo,
                task_repo=self.task_repo,
                tool_repo=self.tool_repo,
                gateway=self.gateway,
                req=req,
                tenant_id="default",
                user=self.user,
                trace_id="trace-episode",
                start_workflow=self.start_workflow,
            )
        self.assertEqual("workflow_task", result["route"])
        self.assertTrue(result["turn"]["agent_run"]["episodes"])
        self.assertEqual("workflow_call", result["turn"]["agent_run"]["policy"]["selected_action"])

    async def test_retryable_tool_failure_replans_to_workflow(self) -> None:
        self.episode_repo.rows = [
            {
                "episode_id": "episode-fallback",
                "normalized_goal": "search temporal workflow docs.python.org",
                "task_summary": "search temporal workflow docs.python.org",
                "chosen_strategy": "workflow_call",
                "action_types": ["workflow_call"],
                "tool_names": ["web_search"],
                "outcome_status": "SUCCEEDED",
                "final_outcome": "Workflow fallback succeeded",
                "useful_lessons": ["Retryable failures should escalate into a different action type."],
            }
        ]
        failing_gateway = AsyncMock(
            return_value={
                "status": "FAILED",
                "reason_code": "adapter_http_429",
                "result": {"error": "rate limited"},
            }
        )
        self.gateway.execute = failing_gateway
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-replan",
            message="search temporal workflow docs.python.org",
            mode="tool_task",
            metadata={},
        )
        with patch(
            "app.services.assistant_orchestration_service.service_create_task",
            AsyncMock(return_value={"task_id": "task-fallback", "run_id": "run-fallback", "status": "QUEUED", "idempotent": False}),
        ):
            result = await assistant_chat(
                conversation_repo=self.conversation_repo,
                episode_repo=self.episode_repo,
                turn_repo=self.turn_repo,
                task_repo=self.task_repo,
                tool_repo=self.tool_repo,
                gateway=self.gateway,
                req=req,
                tenant_id="default",
                user=self.user,
                trace_id="trace-replan",
                start_workflow=self.start_workflow,
        )
        self.assertEqual("workflow_task", result["route"])
        self.assertEqual("task_created", result["response_type"])
        self.assertIn("外部服务当前较忙", result["message"])
        self.assertIn("持续执行任务 task-fallback", result["message"])
        self.assertEqual("replan", result["turn"]["agent_run"]["current_action"]["action_type"])

    async def test_final_tool_failure_uses_natural_chinese_feedback(self) -> None:
        failing_gateway = AsyncMock(
            return_value={
                "status": "FAILED",
                "reason_code": "tool_denied",
                "result": {"error": "permission denied"},
            }
        )
        self.gateway.execute = failing_gateway
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-final-failure",
            message="search temporal workflow docs.python.org",
            mode="tool_task",
            metadata={},
        )
        result = await assistant_chat(
            conversation_repo=self.conversation_repo,
            episode_repo=self.episode_repo,
            turn_repo=self.turn_repo,
            task_repo=self.task_repo,
            tool_repo=self.tool_repo,
            gateway=self.gateway,
            req=req,
            tenant_id="default",
            user=self.user,
            trace_id="trace-final-failure",
            start_workflow=self.start_workflow,
        )
        self.assertEqual("tool_task", result["route"])
        self.assertEqual("direct_answer", result["response_type"])
        self.assertIn("更高权限或额外确认", result["message"])
        self.assertNotIn("tool_denied", result["message"])

    async def test_retry_heavy_episode_biases_initial_route_to_workflow(self) -> None:
        self.episode_repo.rows = [
            {
                "episode_id": "episode-tool-retry",
                "normalized_goal": "search temporal workflow docs.python.org",
                "task_summary": "search temporal workflow docs.python.org",
                "chosen_strategy": "tool_call",
                "action_types": ["tool_call", "workflow_call"],
                "tool_names": ["web_search"],
                "outcome_status": "FAILED_RETRYABLE",
                "final_outcome": "Fast tool path failed before workflow fallback.",
                "useful_lessons": ["Retryable failures should escalate into a different action type."],
            }
        ]
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-retry-bias",
            message="search temporal workflow docs.python.org",
            mode="auto",
            metadata={},
        )
        with patch(
            "app.services.assistant_orchestration_service.service_create_task",
            AsyncMock(return_value={"task_id": "task-biased", "run_id": "run-biased", "status": "QUEUED", "idempotent": False}),
        ):
            result = await assistant_chat(
                conversation_repo=self.conversation_repo,
                episode_repo=self.episode_repo,
                turn_repo=self.turn_repo,
                task_repo=self.task_repo,
                tool_repo=self.tool_repo,
                gateway=self.gateway,
                req=req,
                tenant_id="default",
                user=self.user,
                trace_id="trace-retry-bias",
                start_workflow=self.start_workflow,
            )
        self.assertEqual("workflow_task", result["route"])
        self.assertEqual("task_created", result["response_type"])
        self.assertEqual("workflow_call", result["turn"]["agent_run"]["current_action"]["action_type"])
        self.assertEqual(1, result["turn"]["agent_run"]["policy"]["experience_profile"]["tool_retry_failures"])

    async def test_user_message_resumes_waiting_goal_identity(self) -> None:
        self.goal_repo.rows["goal-resume-1"] = {
            "goal_id": "goal-resume-1",
            "tenant_id": "default",
            "user_id": self.user["id"],
            "conversation_id": "conv-resume",
            "normalized_goal": "prepare launch brief",
            "status": "WAITING",
            "current_task_id": "task-old",
            "last_turn_id": "turn-old",
            "continuation_count": 2,
            "goal_state": {
                "goal": {
                    "goal_id": "goal-resume-1",
                    "normalized_goal": "prepare launch brief",
                    "success_criteria": ["Collect data", "Draft brief"],
                },
                "planner": {"task_type": "research_summary"},
                "task_state": {"current_phase": "wait"},
                "current_action": {"action_type": "ask_user"},
                "policy": {"selected_action": "ask_user"},
                "reflection": {"next_action": "workflow_call"},
                "wake_condition": {"kind": "user_message", "event_key": "conv-resume", "resume_action": "workflow_call"},
                "active_subgoal": {"subgoal_id": "goal-resume-1:sg:1", "sequence_no": 1, "title": "Collect data", "status": "WAITING"},
            },
        }
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-resume",
            message="I added the stakeholder metrics",
            mode="workflow_task",
            metadata={},
        )
        with patch(
            "app.services.assistant_orchestration_service.service_create_task",
            AsyncMock(return_value={"task_id": "task-resumed", "run_id": "run-resumed", "status": "QUEUED", "idempotent": False}),
        ) as mocked_create_task:
            result = await assistant_chat(
                conversation_repo=self.conversation_repo,
                episode_repo=self.episode_repo,
                turn_repo=self.turn_repo,
                task_repo=self.task_repo,
                tool_repo=self.tool_repo,
                gateway=self.gateway,
                req=req,
                tenant_id="default",
                user=self.user,
                trace_id="trace-resume",
                start_workflow=self.start_workflow,
                goal_repo=self.goal_repo,
            )
        workflow_req = mocked_create_task.await_args.kwargs["req"]
        self.assertEqual("goal-resume-1", workflow_req.goal_id)
        self.assertEqual("prepare launch brief", workflow_req.input["goal"]["normalized_goal"])
        self.assertEqual("goal_resume", result["turn"]["agent_run"]["observations"][-1]["kind"])

    async def test_low_risk_goal_canary_policy_is_selected(self) -> None:
        policy_repo = _FakePolicyRepo()
        policy_repo.create_policy_version(
            tenant_id="default",
            version_id="policy-active",
            version_tag="active",
            status="ACTIVE",
            base_version_id=None,
            source="bootstrap",
            memory_payload={"eval_summary": {"success_rate": 0.95}},
            comparison_payload={},
            created_by=self.user["id"],
        )
        policy_repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id="policy-active",
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.97}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by=self.user["id"],
        )
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-canary",
            message="Please prepare a research summary report",
            mode="workflow_task",
            metadata={},
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_canary_allow_high_risk", False),
            patch.object(settings, "policy_shadow_enabled", False),
            patch(
                "app.services.assistant_orchestration_service.service_create_task",
                AsyncMock(return_value={"task_id": "task-canary", "run_id": "run-canary", "status": "QUEUED", "idempotent": False}),
            ) as mocked_create_task,
        ):
            result = await assistant_chat(
                conversation_repo=self.conversation_repo,
                episode_repo=self.episode_repo,
                turn_repo=self.turn_repo,
                task_repo=self.task_repo,
                tool_repo=self.tool_repo,
                gateway=self.gateway,
                req=req,
                tenant_id="default",
                user=self.user,
                trace_id="trace-canary",
                start_workflow=self.start_workflow,
                policy_repo=policy_repo,
                goal_repo=self.goal_repo,
            )

        workflow_req = mocked_create_task.await_args.kwargs["req"]
        self.assertEqual("policy-canary", workflow_req.input["runtime_state"]["policy"]["policy_version_id"])
        self.assertEqual("canary", workflow_req.input["runtime_state"]["policy"]["policy_selector"]["mode"])
        self.assertEqual("policy-canary", self.goal_repo.rows[workflow_req.goal_id]["policy_version_id"])

    async def test_active_policy_run_records_shadow_probe_for_canary(self) -> None:
        policy_repo = _FakePolicyRepo()
        policy_repo.create_policy_version(
            tenant_id="default",
            version_id="policy-active",
            version_tag="active",
            status="ACTIVE",
            base_version_id=None,
            source="bootstrap",
            memory_payload={"eval_summary": {"success_rate": 0.95}},
            comparison_payload={},
            created_by=self.user["id"],
        )
        policy_repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id="policy-active",
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.97}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by=self.user["id"],
        )
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-shadow-active",
            message="Please prepare a research summary report",
            mode="workflow_task",
            metadata={},
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_canary_allow_high_risk", False),
            patch.object(settings, "policy_shadow_enabled", True),
            patch.object(settings, "policy_shadow_min_probe_count", 2),
            patch(
                "app.services.assistant_orchestration_service.service_create_task",
                AsyncMock(return_value={"task_id": "task-shadow-active", "run_id": "run-shadow-active", "status": "QUEUED", "idempotent": False}),
            ) as mocked_create_task,
        ):
            await assistant_chat(
                conversation_repo=self.conversation_repo,
                episode_repo=self.episode_repo,
                turn_repo=self.turn_repo,
                task_repo=self.task_repo,
                tool_repo=self.tool_repo,
                gateway=self.gateway,
                req=req,
                tenant_id="default",
                user=self.user,
                trace_id="trace-shadow-active",
                start_workflow=self.start_workflow,
                policy_repo=policy_repo,
                goal_repo=self.goal_repo,
            )

        workflow_req = mocked_create_task.await_args.kwargs["req"]
        self.assertEqual("policy-active", workflow_req.input["runtime_state"]["policy"]["policy_version_id"])
        self.assertEqual("active", workflow_req.input["runtime_state"]["policy"]["policy_selector"]["mode"])
        self.assertEqual(
            1,
            policy_repo.rows["policy-canary"]["comparison_payload"]["shadow_probe_counts"]["total"],
        )
        self.assertEqual(
            "policy-canary",
            workflow_req.input["runtime_state"]["policy"]["shadow_policy"]["version_id"],
        )

    async def test_resumed_goal_preserves_existing_policy_version(self) -> None:
        policy_repo = _FakePolicyRepo()
        policy_repo.create_policy_version(
            tenant_id="default",
            version_id="policy-active",
            version_tag="active",
            status="ACTIVE",
            base_version_id=None,
            source="bootstrap",
            memory_payload={"eval_summary": {"success_rate": 0.95}},
            comparison_payload={},
            created_by=self.user["id"],
        )
        policy_repo.create_policy_version(
            tenant_id="default",
            version_id="policy-canary",
            version_tag="canary",
            status="CANARY",
            base_version_id="policy-active",
            source="eval_feedback",
            memory_payload={"eval_summary": {"success_rate": 0.97}},
            comparison_payload={"last_eval_verdict": {"passed": True}},
            created_by=self.user["id"],
        )
        self.goal_repo.rows["goal-policy-1"] = {
            "goal_id": "goal-policy-1",
            "tenant_id": "default",
            "user_id": self.user["id"],
            "conversation_id": "conv-policy",
            "normalized_goal": "prepare launch brief",
            "status": "WAITING",
            "current_task_id": "task-old",
            "last_turn_id": "turn-old",
            "continuation_count": 2,
            "policy_version_id": "policy-active",
            "goal_state": {
                "goal": {
                    "goal_id": "goal-policy-1",
                    "normalized_goal": "prepare launch brief",
                    "success_criteria": ["Collect data", "Draft brief"],
                },
                "planner": {"task_type": "research_summary"},
                "task_state": {"current_phase": "wait"},
                "current_action": {"action_type": "ask_user"},
                "policy": {"selected_action": "ask_user", "policy_version_id": "policy-active"},
                "reflection": {"next_action": "workflow_call"},
                "wake_condition": {"kind": "user_message", "event_key": "conv-policy", "resume_action": "workflow_call"},
                "active_subgoal": {"subgoal_id": "goal-policy-1:sg:1", "sequence_no": 1, "title": "Collect data", "status": "WAITING"},
            },
        }
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-policy",
            message="I attached the stakeholder metrics",
            mode="workflow_task",
            metadata={},
        )

        with (
            patch.object(settings, "policy_canary_enabled", True),
            patch.object(settings, "policy_canary_rollout_pct", 100),
            patch.object(settings, "policy_shadow_enabled", False),
            patch(
                "app.services.assistant_orchestration_service.service_create_task",
                AsyncMock(return_value={"task_id": "task-policy", "run_id": "run-policy", "status": "QUEUED", "idempotent": False}),
            ) as mocked_create_task,
        ):
            result = await assistant_chat(
                conversation_repo=self.conversation_repo,
                episode_repo=self.episode_repo,
                turn_repo=self.turn_repo,
                task_repo=self.task_repo,
                tool_repo=self.tool_repo,
                gateway=self.gateway,
                req=req,
                tenant_id="default",
                user=self.user,
                trace_id="trace-policy",
                start_workflow=self.start_workflow,
                policy_repo=policy_repo,
                goal_repo=self.goal_repo,
            )

        workflow_req = mocked_create_task.await_args.kwargs["req"]
        self.assertEqual("policy-active", workflow_req.input["runtime_state"]["policy"]["policy_version_id"])
        self.assertEqual("goal_continuity", workflow_req.input["runtime_state"]["policy"]["policy_selector"]["reason"])
        self.assertEqual("policy-active", self.goal_repo.rows["goal-policy-1"]["policy_version_id"])

    async def test_retryable_tool_failure_stays_inline_for_auto_web_lookup(self) -> None:
        failing_gateway = AsyncMock(
            return_value={
                "status": "FAILED",
                "reason_code": "adapter_http_429",
                "result": {"error": "rate limited"},
            }
        )
        self.gateway.execute = failing_gateway
        req = AssistantChatRequest(
            user_id=self.user["id"],
            conversation_id="conv-inline-retry",
            message="\u8bf7\u5e2e\u6211\u641c\u7d22 workflow \u6587\u6863",
            mode="auto",
            metadata={},
        )

        result = await assistant_chat(
            conversation_repo=self.conversation_repo,
            episode_repo=self.episode_repo,
            turn_repo=self.turn_repo,
            task_repo=self.task_repo,
            tool_repo=self.tool_repo,
            gateway=self.gateway,
            req=req,
            tenant_id="default",
            user=self.user,
            trace_id="trace-inline-retry",
            start_workflow=self.start_workflow,
            goal_repo=self.goal_repo,
        )

        self.assertEqual("tool_task", result["route"])
        self.assertEqual("direct_answer", result["response_type"])
        self.assertIsNotNone(result["task"])
        self.assertEqual("FAILED_RETRYABLE", result["task"]["status"])


if __name__ == "__main__":
    unittest.main()
