import unittest
from unittest.mock import patch

from app.repositories import TaskRepository


class TaskFailureFieldTests(unittest.TestCase):
    def test_mark_task_failed_persists_normalized_code_and_masked_truncated_message(self) -> None:
        repo = TaskRepository()
        raw_message = {
            "password": "super-secret",
            "token": "abcdef",
            "detail": "x" * 5000,
        }
        with patch("app.repositories.execute", return_value=1) as execute:
            repo.mark_task_failed(
                tenant_id="default",
                task_id="task-1",
                status_text="FAILED_FINAL",
                error_code="adapter_http_5xx",
                error_message=raw_message,
            )

        self.assertEqual(1, execute.call_count)
        query = str(execute.call_args.args[0])
        params = tuple(execute.call_args.args[1])
        self.assertIn("error_code", query)
        self.assertIn("error_message", query)
        self.assertEqual("FAILED_FINAL", params[0])
        self.assertEqual("adapter_http_5xx", params[1])
        self.assertLessEqual(len(params[2]), 2048)
        self.assertIn("***", params[2])
        self.assertNotIn("super-secret", params[2])
        self.assertNotIn("abcdef", params[2])


if __name__ == "__main__":
    unittest.main()
