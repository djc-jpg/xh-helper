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


if __name__ == "__main__":
    unittest.main()
