import json
import unittest

from apps.worker.mas.messaging import AgentMessage, EventBus, InMemoryMessageQueue


class MessagingProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_event_bus_emits_required_protocol_fields(self) -> None:
        bus = EventBus(InMemoryMessageQueue())

        message = await bus.send_message(
            sender="scheduler_agent",
            receiver="execution_agent",
            topic="execution.requested",
            payload={"task_id": "t-100", "run_id": "r-100"},
            task_id="t-100",
            run_id="r-100",
            correlation_id="corr-100",
        )

        self.assertEqual("t-100", message.task_id)
        self.assertEqual("r-100", message.run_id)
        self.assertEqual("corr-100", message.correlation_id)
        self.assertEqual("execution.requested", message.topic)
        self.assertIsInstance(message.payload, dict)
        self.assertIsInstance(message.timestamp, float)
        self.assertGreater(message.timestamp, 0.0)

    async def test_event_bus_can_infer_run_id_from_nested_payload_task(self) -> None:
        bus = EventBus(InMemoryMessageQueue())

        message = await bus.send_message(
            sender="approval_agent",
            receiver="execution_agent",
            topic="approval.granted",
            payload={"task": {"task_id": "t-101", "run_id": "r-101"}},
            task_id="t-101",
            correlation_id="corr-101",
        )

        self.assertEqual("r-101", message.run_id)

    def test_from_json_supports_legacy_created_at(self) -> None:
        raw = json.dumps(
            {
                "message_id": "m-legacy",
                "topic": "legacy.topic",
                "sender": "a",
                "receiver": "b",
                "task_id": "t-legacy",
                "correlation_id": "c-legacy",
                "payload": {"ok": True},
                "created_at": 1234.5,
                "priority": 1,
            }
        )

        message = AgentMessage.from_json(raw)
        self.assertEqual("t-legacy", message.task_id)
        self.assertIsNone(message.run_id)
        self.assertEqual(1234.5, message.timestamp)


if __name__ == "__main__":
    unittest.main()
