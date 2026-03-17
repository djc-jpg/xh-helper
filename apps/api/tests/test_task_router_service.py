import unittest

from app.services.task_router_service import TaskRouterService


class TaskRouterServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.router = TaskRouterService()

    def test_routes_short_question_to_direct_answer(self) -> None:
        decision = self.router.route(message="What can you do?", mode=None, metadata={}, history=[])
        self.assertEqual("direct_answer", decision.route)

    def test_routes_search_intent_to_tool_task(self) -> None:
        decision = self.router.route(
            message="search async context manager docs.python.org",
            mode=None,
            metadata={},
            history=[],
        )
        self.assertEqual("tool_task", decision.route)
        self.assertEqual("web_search", decision.tool_id)
        self.assertEqual("docs.python.org", decision.tool_payload["domain"])

    def test_routes_ticket_intent_to_workflow_task(self) -> None:
        decision = self.router.route(
            message="Please create a ticket email for this incident",
            mode=None,
            metadata={},
            history=[],
        )
        self.assertEqual("workflow_task", decision.route)
        self.assertEqual("ticket_email", decision.task_type)

    def test_mode_can_force_route(self) -> None:
        decision = self.router.route(message="hello", mode="workflow_task", metadata={}, history=[])
        self.assertEqual("workflow_task", decision.route)


if __name__ == "__main__":
    unittest.main()
