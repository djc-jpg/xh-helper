import unittest
from unittest.mock import AsyncMock, patch

from app.services.planner_service import PlannerService


class PlannerServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.planner = PlannerService()
        self.low_risk_tool = {
            "tool_name": "web_search",
            "risk_level": "low",
            "requires_approval": False,
        }
        self.high_risk_tool = {
            "tool_name": "email_ticketing",
            "risk_level": "high",
            "requires_approval": True,
        }

    def test_outputs_use_retrieval_for_question_with_hits(self) -> None:
        plan = self.planner.plan(
            message="How does approval outbox work?",
            mode=None,
            metadata={},
            history=[],
            memory={},
            retrieval_hits=[{"title": "approval-outbox", "snippet": "..."}, {"title": "architecture", "snippet": "..."}],
            tool_candidates=[],
        )
        self.assertEqual("use_retrieval", plan["action"])
        self.assertEqual("rag_qa", plan["task_type"])
        self.assertTrue(plan["confidence"] >= 0.7)
        self.assertEqual("retrieve", plan["policy_signals"]["action_signal"])
        self.assertGreaterEqual(plan["policy_signals"]["action_affinities"]["retrieve"], 0.7)

    def test_outputs_need_approval_for_high_risk_tool(self) -> None:
        plan = self.planner.plan(
            message="send email ticket to security now",
            mode="tool_task",
            metadata={},
            history=[],
            memory={},
            retrieval_hits=[],
            tool_candidates=[self.high_risk_tool],
        )
        self.assertEqual("need_approval", plan["action"])
        self.assertTrue(plan["need_confirmation"])
        self.assertIn("email_ticketing", plan["tool_candidates"])
        self.assertTrue(plan["policy_signals"]["requires_approval"])
        self.assertEqual("approval_request", plan["policy_signals"]["action_signal"])

    def test_outputs_use_tool_for_tool_hint(self) -> None:
        plan = self.planner.plan(
            message="search python context manager docs",
            mode=None,
            metadata={},
            history=[],
            memory={},
            retrieval_hits=[],
            tool_candidates=[self.low_risk_tool],
        )
        self.assertEqual("use_tool", plan["action"])
        self.assertEqual("web_search", plan["selected_tool"])
        self.assertEqual("tool_call", plan["policy_signals"]["action_signal"])

    def test_explanatory_workflow_question_stays_in_answer_path(self) -> None:
        plan = self.planner.plan(
            message="How does the workflow runtime work in this repo?",
            mode=None,
            metadata={},
            history=[],
            memory={},
            retrieval_hits=[{"title": "product", "snippet": "..."}],
            tool_candidates=[self.low_risk_tool],
        )
        self.assertEqual("use_retrieval", plan["action"])
        self.assertEqual("general_qna", plan["intent"])
        self.assertLess(plan["policy_signals"]["action_affinities"]["workflow_call"], 0.5)
        self.assertGreaterEqual(plan["policy_signals"]["action_affinities"]["retrieve"], 0.8)

    def test_chinese_explanatory_question_stays_in_answer_path(self) -> None:
        plan = self.planner.plan(
            message="这个 workflow runtime 是怎么工作的？",
            mode=None,
            metadata={},
            history=[],
            memory={},
            retrieval_hits=[{"title": "product", "snippet": "..."}],
            tool_candidates=[self.low_risk_tool],
        )
        self.assertEqual("use_retrieval", plan["action"])
        self.assertEqual("general_qna", plan["intent"])
        self.assertLess(plan["policy_signals"]["action_affinities"]["workflow_call"], 0.5)
        self.assertGreaterEqual(plan["policy_signals"]["action_affinities"]["retrieve"], 0.8)

    def test_chinese_continue_followup_routes_to_workflow(self) -> None:
        plan = self.planner.plan(
            message="\u8bf7\u53d1\u8d77\u4e00\u4e2a\u6301\u7eed\u6267\u884c\u4efb\u52a1\uff0c\u5e2e\u6211\u7ee7\u7eed\u8ddf\u8fdb\u8fd9\u4e2a\u95ee\u9898\uff0c\u76f4\u5230\u6709\u7ed3\u679c\u518d\u56de\u6765\u3002",
            mode=None,
            metadata={},
            history=[],
            memory={},
            retrieval_hits=[],
            tool_candidates=[self.low_risk_tool],
        )
        self.assertEqual("start_workflow", plan["action"])
        self.assertEqual("workflow_call", plan["policy_signals"]["action_signal"])
        self.assertGreaterEqual(plan["policy_signals"]["action_affinities"]["workflow_call"], 0.55)


class PlannerServiceQwenTests(unittest.IsolatedAsyncioTestCase):
    async def test_aplan_merges_qwen_output(self) -> None:
        planner = PlannerService()
        qwen_chat = AsyncMock(
            return_value=(
                '{"action":"use_tool","task_type":"tool_flow","intent":"knowledge_lookup",'
                '"plan_steps":["Understand request","Use the selected tool","Summarize output"],'
                '"tool_candidates":["web_search"],"selected_tool":"web_search","need_confirmation":false,"confidence":0.88}'
            )
        )
        with (
            patch("app.services.planner_service.qwen_client.is_enabled", return_value=True),
            patch("app.services.planner_service.qwen_client.chat_text", new=qwen_chat),
        ):
            plan = await planner.aplan(
                message="search python context manager docs",
                mode=None,
                metadata={},
                history=[],
                memory={},
                retrieval_hits=[],
                tool_candidates=[{"tool_name": "web_search", "risk_level": "low", "requires_approval": False}],
            )

        self.assertEqual("use_tool", plan["action"])
        self.assertEqual("tool_flow", plan["task_type"])
        self.assertEqual("web_search", plan["selected_tool"])
        self.assertEqual(["Understand request", "Use the selected tool", "Summarize output"], plan["plan_steps"])
        self.assertEqual(0.88, plan["confidence"])
        self.assertEqual("tool_call", plan["policy_signals"]["action_signal"])
        self.assertEqual(8.0, qwen_chat.await_args.kwargs["timeout_s"])

    async def test_aplan_keeps_explanatory_question_out_of_workflow(self) -> None:
        planner = PlannerService()
        qwen_chat = AsyncMock(
            return_value=(
                '{"action":"start_workflow","task_type":"research_summary","intent":"task_execution",'
                '"plan_steps":["Start a workflow"],"tool_candidates":["web_search"],'
                '"selected_tool":"web_search","need_confirmation":false,"confidence":0.86}'
            )
        )
        with (
            patch("app.services.planner_service.qwen_client.is_enabled", return_value=True),
            patch("app.services.planner_service.qwen_client.chat_text", new=qwen_chat),
        ):
            plan = await planner.aplan(
                message="How does the workflow runtime work in this repo?",
                mode=None,
                metadata={},
                history=[],
                memory={},
                retrieval_hits=[{"title": "product", "snippet": "..."}],
                tool_candidates=[{"tool_name": "web_search", "risk_level": "low", "requires_approval": False}],
            )

        self.assertEqual("use_retrieval", plan["action"])


if __name__ == "__main__":
    unittest.main()
