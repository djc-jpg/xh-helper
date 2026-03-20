import unittest

from app.services.assistant_runtime_service import build_turn_summary


class AssistantRuntimeServiceTests(unittest.TestCase):
    def test_build_turn_summary_normalizes_legacy_runtime_steps(self) -> None:
        summary = build_turn_summary(
            {
                "turn_id": "turn-legacy-1",
                "route": "workflow_task",
                "status": "RUNNING",
                "current_phase": "plan",
                "response_type": "task_created",
                "user_message": "send ticket to oncall team",
                "assistant_message": "正在处理",
                "trace_id": "trace-legacy-1",
                "runtime_state": {
                    "steps": [
                        {
                            "phase": "plan",
                            "title": "Choose next action",
                            "summary": "Policy selected workflow_call.",
                        }
                    ]
                },
            }
        )

        steps = summary["agent_run"]["steps"]
        self.assertEqual(1, len(steps))
        self.assertEqual("choose_next_action", steps[0]["key"])
        self.assertEqual("completed", steps[0]["status"])
        self.assertEqual("Choose next action", steps[0]["title"])


if __name__ == "__main__":
    unittest.main()
