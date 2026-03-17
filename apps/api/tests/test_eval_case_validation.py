import unittest

from eval.run_eval import evaluate_assistant_trace, validate_case_expectations


class EvalCaseValidationTests(unittest.TestCase):
    def test_accepts_required_force_cases(self) -> None:
        cases = [
            {"id": "force_400", "expect_status": "FAILED_FINAL", "input": {"query": "anything"}},
            {"id": "force_500", "expect_status": "FAILED_RETRYABLE", "input": {"query": "anything"}},
            {"id": "ok-success", "expect_status": "SUCCEEDED", "input": {"query": "all"}},
        ]
        validate_case_expectations(cases)

    def test_rejects_wrong_force_400_status(self) -> None:
        cases = [
            {"id": "force_400", "expect_status": "FAILED_RETRYABLE", "input": {"query": "anything"}},
            {"id": "force_500", "expect_status": "FAILED_RETRYABLE", "input": {"query": "anything"}},
        ]
        with self.assertRaises(ValueError):
            validate_case_expectations(cases)

    def test_rejects_missing_required_force_case(self) -> None:
        cases = [
            {"id": "force_400", "expect_status": "FAILED_FINAL", "input": {"query": "anything"}},
            {"id": "ok-success", "expect_status": "SUCCEEDED", "input": {"query": "all"}},
        ]
        with self.assertRaises(ValueError):
            validate_case_expectations(cases)

    def test_rejects_when_required_case_id_is_renamed(self) -> None:
        cases = [
            {"id": "tool_update_1", "expect_status": "FAILED_FINAL", "input": {"query": "force_400"}},
            {"id": "force_500", "expect_status": "FAILED_RETRYABLE", "input": {"query": "force_500"}},
        ]
        with self.assertRaises(ValueError):
            validate_case_expectations(cases)

    def test_accepts_assistant_case_requirements(self) -> None:
        cases = [
            {"id": "force_400", "expect_status": "FAILED_FINAL", "input": {"query": "force_400"}},
            {"id": "force_500", "expect_status": "FAILED_RETRYABLE", "input": {"query": "force_500"}},
            {
                "id": "assistant_runtime_1",
                "mode": "assistant",
                "role": "user",
                "input": {"message": "Please prepare a grounded summary.", "conversation_id": "conv-agent"},
                "expect_response_type": "task_created",
                "expect_task_trace": True,
                "expect_runtime_debugger": True,
                "expected_why_not_keys": ["respond"],
            },
        ]
        validate_case_expectations(cases)

    def test_rejects_assistant_case_without_message(self) -> None:
        cases = [
            {"id": "force_400", "expect_status": "FAILED_FINAL", "input": {"query": "force_400"}},
            {"id": "force_500", "expect_status": "FAILED_RETRYABLE", "input": {"query": "force_500"}},
            {
                "id": "assistant_runtime_1",
                "mode": "assistant",
                "role": "user",
                "input": {"conversation_id": "conv-agent"},
                "expect_response_type": "task_created",
            },
        ]
        with self.assertRaises(ValueError):
            validate_case_expectations(cases)

    def test_evaluate_assistant_trace_checks_runtime_debugger_and_why_not(self) -> None:
        result = evaluate_assistant_trace(
            case={
                "expect_task_trace": True,
                "expect_runtime_debugger": True,
                "expected_why_not_keys": ["respond"],
            },
            response_payload={
                "task": {"task_id": "task-1"},
            },
            trace_payload={
                "goal": {"normalized_goal": "prepare summary"},
                "current_action": {"action_type": "workflow_call"},
                "policy": {"selected_action": "workflow_call"},
                "runtime_debugger": {"why_not": {"respond": "Need durable execution first."}},
            },
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["trace_required"])
        self.assertTrue(result["trace_present"])
        self.assertTrue(result["runtime_debugger_present"])
        self.assertTrue(result["policy_trace_present"])
        self.assertTrue(result["why_not_present"])


if __name__ == "__main__":
    unittest.main()
