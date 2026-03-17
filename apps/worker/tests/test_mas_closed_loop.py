import unittest
from unittest.mock import patch

from apps.worker.mas.closed_loop import ClosedLoopCoordinator, REQUIRED_PROTOCOL_FIELDS


class ClosedLoopCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    TEN_AGENT_IDS = {
        "perceptor_agent",
        "planner_agent",
        "scheduler_agent",
        "approval_agent",
        "knowledge_resolution_agent",
        "researcher_agent",
        "weather_agent",
        "writer_agent",
        "execution_agent",
        "critic_agent",
    }

    async def test_protocol_messages_have_required_fields(self) -> None:
        coordinator = ClosedLoopCoordinator(default_retry_budget=1, default_latency_budget_ms=20000)
        result = await coordinator.run(
            {
                "task_id": "t-1",
                "run_id": "r-1",
                "task_type": "research_summary",
                "input": {"query": "find key points", "success_criteria": ["non_empty_output"]},
            }
        )
        self.assertEqual("SUCCEEDED", result["status"])
        self.assertTrue(result["protocol_messages"])
        seen_agents = {str(message.get("agent") or "") for message in result["protocol_messages"]}
        self.assertTrue(self.TEN_AGENT_IDS.issubset(seen_agents))
        metrics = dict((result.get("state") or {}).get("metrics") or {})
        self.assertEqual("langgraph", metrics.get("graph_engine"))
        node_calls = dict(metrics.get("graph_node_calls") or {})
        self.assertGreaterEqual(int(node_calls.get("scheduler1") or 0), 1)
        self.assertGreaterEqual(int(node_calls.get("scheduler2") or 0), 1)
        runtime = dict((result.get("state") or {}).get("agent_runtime") or {})
        self.assertEqual("SUCCEEDED", runtime.get("status"))
        self.assertEqual("respond", runtime.get("current_phase"))
        self.assertEqual("respond", ((runtime.get("current_action") or {}).get("action_type")))
        self.assertTrue(list(runtime.get("steps") or []))
        for message in result["protocol_messages"]:
            self.assertTrue(REQUIRED_PROTOCOL_FIELDS.issubset(set(message.keys())))

    async def test_missing_required_input_returns_need_info(self) -> None:
        coordinator = ClosedLoopCoordinator(default_retry_budget=1, default_latency_budget_ms=20000)
        result = await coordinator.run(
            {
                "task_id": "t-2",
                "run_id": "r-2",
                "task_type": "rag_qa",
                "input": {},
            }
        )
        self.assertEqual("FAILED_FINAL", result["status"])
        self.assertEqual("NEED_INFO", result["failure_type"])
        self.assertEqual("FAIL_FINAL", result["failure_semantic"])
        runtime = dict((result.get("state") or {}).get("agent_runtime") or {})
        self.assertEqual("ask_user", ((runtime.get("current_action") or {}).get("action_type")))
        self.assertEqual("ask_user", runtime.get("current_phase"))
        self.assertIn("goal/query", list(((runtime.get("task_state") or {}).get("unknowns") or [])))

    async def test_critic_fail_then_replan_then_pass(self) -> None:
        coordinator = ClosedLoopCoordinator(default_retry_budget=1, default_latency_budget_ms=20000)
        result = await coordinator.run(
            {
                "task_id": "t-3",
                "run_id": "r-3",
                "task_type": "tool_flow",
                "input": {
                    "query": "generate response",
                    "success_criteria": ["must_include:ticket_id"],
                },
            }
        )
        self.assertEqual("SUCCEEDED", result["status"])
        self.assertEqual(2, int(result["turn"]))
        metrics = dict((result.get("state") or {}).get("metrics") or {})
        self.assertEqual("langgraph", metrics.get("graph_engine"))
        agent_counts = dict(metrics.get("agent_message_counts") or {})
        self.assertGreaterEqual(int(agent_counts.get("critic_agent") or 0), 2)
        self.assertGreaterEqual(int(agent_counts.get("planner_agent") or 0), 2)
        output = str((result.get("result") or {}).get("output") or "")
        self.assertIn("ticket_id", output)
        runtime = dict((result.get("state") or {}).get("agent_runtime") or {})
        self.assertEqual("STABLE", ((result.get("state") or {}).get("plan_state")))
        self.assertEqual("respond", runtime.get("current_phase"))
        self.assertEqual("respond", ((runtime.get("current_action") or {}).get("action_type")))

    async def test_retry_budget_exhaustion_returns_fail_final(self) -> None:
        coordinator = ClosedLoopCoordinator(default_retry_budget=0, default_latency_budget_ms=20000)
        result = await coordinator.run(
            {
                "task_id": "t-4",
                "run_id": "r-4",
                "task_type": "research_summary",
                "input": {"query": "force_500", "retry_budget": 0},
            }
        )
        self.assertEqual("FAILED_FINAL", result["status"])
        self.assertEqual("RETRY_BUDGET_EXHAUSTED", result["failure_type"])
        self.assertEqual("FAIL_FINAL", result["failure_semantic"])

    async def test_protocol_error_translated_to_terminal_state(self) -> None:
        coordinator = ClosedLoopCoordinator(default_retry_budget=1, default_latency_budget_ms=20000)
        with patch("apps.worker.mas.langgraph_graph.validate_protocol_message", side_effect=ValueError("bad protocol")):
            result = await coordinator.run(
                {
                    "task_id": "t-5",
                    "run_id": "r-5",
                    "task_type": "research_summary",
                    "input": {"query": "hello"},
                }
            )
        self.assertEqual("FAILED_RETRYABLE", result["status"])
        self.assertEqual("PROTOCOL_ERROR", result["failure_type"])
        self.assertEqual("FAIL_RETRYABLE", result["failure_semantic"])
        self.assertIn("protocol_error", str(result.get("reason") or ""))

    async def test_runtime_seed_is_preserved_across_closed_loop(self) -> None:
        coordinator = ClosedLoopCoordinator(default_retry_budget=1, default_latency_budget_ms=20000)
        result = await coordinator.run(
            {
                "task_id": "t-6",
                "run_id": "r-6",
                "task_type": "research_summary",
                "input": {
                    "query": "summarize the issue",
                    "runtime_state": {
                        "episodes": [{"episode_id": "ep-1", "chosen_strategy": "workflow_call"}],
                        "decision": {"route": "workflow_task", "summary": "handoff from assistant"},
                    },
                },
            }
        )
        runtime = dict((result.get("state") or {}).get("agent_runtime") or {})
        self.assertEqual("ep-1", ((runtime.get("episodes") or [{}])[0].get("episode_id")))
        self.assertEqual("workflow_task", ((runtime.get("decision") or {}).get("route")))


if __name__ == "__main__":
    unittest.main()
