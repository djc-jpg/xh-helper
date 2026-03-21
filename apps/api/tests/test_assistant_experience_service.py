import unittest

from app.services.assistant_experience_service import build_conversation_summary, build_task_card


class AssistantExperienceServiceTests(unittest.TestCase):
    def test_waiting_task_card_uses_natural_assistant_summary(self) -> None:
        card = build_task_card(
            {
                "id": "task-1",
                "task_type": "research_summary",
                "status": "WAITING_HUMAN",
                "latest_step_key": "assistant_tool_run",
                "tool_call_count": 1,
                "waiting_approval_count": 1,
                "trace_id": "trace-1",
            }
        )

        self.assertEqual("等待确认", card["chat_state"])
        self.assertEqual("等待人工确认", card["waiting_for"])
        self.assertIn("这一步正在等待人工确认", card["progress_message"])
        self.assertIn("当前在等待人工确认", card["assistant_summary"])

    def test_failed_task_card_maps_error_code_into_human_copy(self) -> None:
        card = build_task_card(
            {
                "id": "task-2",
                "task_type": "tool_flow",
                "status": "FAILED_FINAL",
                "error_code": "adapter_http_429",
                "error_message": "adapter busy",
                "tool_call_count": 1,
                "waiting_approval_count": 0,
                "trace_id": "trace-2",
            }
        )

        self.assertEqual("失败", card["chat_state"])
        self.assertEqual("外部服务当前较忙，请稍后重试。", card["failure_reason"])
        self.assertIn("这次处理没有顺利完成", card["assistant_summary"])

    def test_conversation_preview_prefers_waiting_copy(self) -> None:
        summary = build_conversation_summary(
            {
                "conversation_id": "conv-1",
                "message_history": [
                    {"role": "user", "message": "帮我继续处理"},
                    {"role": "assistant", "message": "好的，我继续看看。", "route": "direct_answer"},
                ],
                "title": None,
                "task_count": 1,
                "running_task_count": 0,
                "waiting_approval_count": 1,
            }
        )

        self.assertEqual("帮我继续处理", summary["title"])
        self.assertEqual("这条对话里有任务正在等待人工确认。", summary["preview"])

    def test_succeeded_task_card_prefers_nested_output_text_over_dict_repr(self) -> None:
        card = build_task_card(
            {
                "id": "task-3",
                "task_type": "ticket_email",
                "status": "SUCCEEDED",
                "output_masked": {
                    "output": "The email ticketing workflow task completed successfully."
                },
                "trace_id": "trace-3",
            }
        )

        self.assertEqual("The email ticketing workflow task completed successfully.", card["result_preview"])
        self.assertEqual("The email ticketing workflow task completed successfully.", card["assistant_summary"])


if __name__ == "__main__":
    unittest.main()
