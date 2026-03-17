import unittest
from unittest.mock import patch

import graph


class GraphQwenTests(unittest.TestCase):
    def test_planner_node_uses_qwen_steps_when_available(self) -> None:
        state = {
            "task_type": "research_summary",
            "input": {
                "query": "summarize workflow durability",
                "goal": {"normalized_goal": "summarize workflow durability", "risk_level": "low"},
                "task_state": {"current_phase": "interpret", "available_actions": ["retrieve", "workflow_call"]},
                "current_action": {"action_type": "workflow_call", "target": "web_search"},
                "policy": {"selected_action": "workflow_call"},
                "episodes": [{"episode_id": "episode-1"}],
            },
            "model_hint": "qwen-plus",
        }
        with (
            patch("graph.qwen_client.is_enabled", return_value=True),
            patch(
                "graph.qwen_client.chat_text",
                return_value="Review prior context\nSearch for evidence\nSummarize durable workflow behavior",
            ),
        ):
            result = graph.planner_node(state)

        self.assertEqual(
            [
                "Review prior context",
                "Search for evidence",
                "Summarize durable workflow behavior",
            ],
            result["plan"],
        )
        self.assertEqual("summarize workflow durability", result["goal"]["normalized_goal"])
        self.assertEqual("workflow_call", result["current_action"]["action_type"])
        self.assertEqual("workflow_call", result["policy"]["selected_action"])

    def test_tool_node_honors_runtime_directed_action(self) -> None:
        state = {
            "task_type": "research_summary",
            "input": {
                "message": "look up runtime policies",
                "metadata": {"domain": "docs.python.org"},
            },
            "current_action": {
                "action_type": "tool_call",
                "target": "web_search",
                "input": {"goal": "runtime policies"},
                "requires_approval": False,
            },
            "policy": {"selected_action": "tool_call", "approval_triggered": False},
            "decision": {"selected_tool": "web_search"},
        }
        result = graph.tool_node(state)

        self.assertEqual("web_search", result["tool_plans"][0]["tool_id"])
        self.assertEqual([], result["pending_tool_plans"])
        self.assertIn("runtime-directed", result["draft_output"])

    def test_hitl_node_respects_runtime_approval_policy(self) -> None:
        state = {
            "task_type": "research_summary",
            "current_action": {"action_type": "tool_call", "requires_approval": True},
            "policy": {"approval_triggered": True},
            "pending_tool_plans": [],
            "agent_steps": [],
        }
        result = graph.hitl_node(state)

        self.assertTrue(result["requires_hitl"])

    def test_runtime_seed_prefers_nested_runtime_state(self) -> None:
        seeded = graph._runtime_seed(
            {
                "runtime_state": {
                    "goal": {"normalized_goal": "nested runtime goal"},
                    "current_action": {"action_type": "workflow_call"},
                    "policy": {"selected_action": "workflow_call"},
                }
            }
        )

        self.assertEqual("nested runtime goal", seeded["goal"]["normalized_goal"])
        self.assertEqual("workflow_call", seeded["current_action"]["action_type"])


if __name__ == "__main__":
    unittest.main()
