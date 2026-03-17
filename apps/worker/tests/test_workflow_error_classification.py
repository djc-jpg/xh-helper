import unittest

from temporalio.exceptions import ActivityError, ApplicationError

from apps.worker.failure_classification import classify_failure_status


def _activity_error_with_cause(cause: Exception) -> ActivityError:
    wrapped = ActivityError(
        "Activity task failed",
        scheduled_event_id=1,
        started_event_id=2,
        identity="worker-local",
        activity_type="execute_tools_activity",
        activity_id="activity-1",
        retry_state=None,
    )
    wrapped.__cause__ = cause
    return wrapped


class WorkflowErrorClassificationTests(unittest.TestCase):
    def test_retryable_application_error_wrapped_by_activity_error(self) -> None:
        app_err = ApplicationError(
            "tool_call_http_error status=502 reason=adapter_http_5xx",
            type="ToolCallRetryableError",
            non_retryable=False,
        )
        exc = _activity_error_with_cause(app_err)
        self.assertEqual("FAILED_RETRYABLE", classify_failure_status(exc))

    def test_non_retryable_application_error_wrapped_by_activity_error(self) -> None:
        app_err = ApplicationError(
            "tool_call_http_error status=400 reason=adapter_http_4xx",
            type="ToolCallNonRetryableError",
            non_retryable=True,
        )
        exc = _activity_error_with_cause(app_err)
        self.assertEqual("FAILED_FINAL", classify_failure_status(exc))


if __name__ == "__main__":
    unittest.main()
