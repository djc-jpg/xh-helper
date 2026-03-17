import unittest

from app.services.tool_registry_service import ToolRegistryService


class _FakeRepo:
    def __init__(self) -> None:
        self.rows = [
            {
                "tool_name": "web_search",
                "version": "v1",
                "description": "Search docs corpus",
                "input_schema": {"type": "object"},
                "risk_level": "low",
                "requires_approval": False,
                "supported_use_cases": ["knowledge_lookup", "docs_search"],
                "enabled": True,
            },
            {
                "tool_name": "email_ticketing",
                "version": "v1",
                "description": "Create or send ticket email",
                "input_schema": {"type": "object"},
                "risk_level": "high",
                "requires_approval": True,
                "supported_use_cases": ["ticket_action"],
                "enabled": True,
            },
        ]

    def list_assistant_registry(self, *, tenant_id: str, enabled_only: bool = True, use_case: str | None = None):
        del tenant_id
        rows = list(self.rows)
        if enabled_only:
            rows = [x for x in rows if bool(x.get("enabled"))]
        if use_case:
            rows = [x for x in rows if use_case in x.get("supported_use_cases", [])]
        return rows

    def upsert_assistant_registry(self, **kwargs) -> None:
        del kwargs

    def get_assistant_registry_item(self, *, tenant_id: str, tool_name: str, version: str):
        del tenant_id, version
        for row in self.rows:
            if row["tool_name"] == tool_name:
                return row
        return None


class ToolRegistryServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = ToolRegistryService(_FakeRepo())

    def test_tool_selection_prefers_relevant_tool(self) -> None:
        tools = self.service.list_tools(tenant_id="default")
        candidates = self.service.select_candidates(message="search docs for workflow", tools=tools, limit=2)
        self.assertTrue(candidates)
        self.assertEqual("web_search", candidates[0]["tool_name"])


if __name__ == "__main__":
    unittest.main()
