import unittest

from idempotency import plan_hash


class PlanHashConsistencyTests(unittest.TestCase):
    def test_same_replay_input_produces_same_plan_hash(self) -> None:
        planner_output_first = {
            "task_type": "rag_qa",
            "input": {"question": "what is incident response"},
            "plan": ["retrieve_evidence", "compose_answer_with_citations"],
            "tool_plans": [],
            "pending_tool_plans": [],
        }
        planner_output_rerun = {
            "task_type": "rag_qa",
            "input": {"question": "what is incident response"},
            "plan": ["retrieve_evidence", "compose_answer_with_citations"],
            "tool_plans": [],
            "pending_tool_plans": [],
        }
        self.assertEqual(plan_hash(planner_output_first), plan_hash(planner_output_rerun))


if __name__ == "__main__":
    unittest.main()

