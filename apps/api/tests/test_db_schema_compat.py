import unittest
from unittest.mock import patch

from app.db import ensure_schema_compat


class _FakeCursor:
    def __init__(self, queries: list[str]) -> None:
        self._queries = queries

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, query: str) -> None:
        self._queries.append(query)


class _FakeConnection:
    def __init__(self, queries: list[str]) -> None:
        self._queries = queries

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self):
        return _FakeCursor(self._queries)


class _FakePool:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def connection(self):
        return _FakeConnection(self.queries)


class SchemaCompatTests(unittest.TestCase):
    def test_ensure_schema_compat_repairs_default_tool_registry_metadata(self) -> None:
        fake_pool = _FakePool()
        with patch("app.db.pool", fake_pool):
            ensure_schema_compat()

        combined = "\n".join(fake_pool.queries)
        self.assertIn("UPDATE tool_registry", combined)
        self.assertIn("tool_id = 'email_ticketing'", combined)
        self.assertIn("requires_approval = TRUE", combined)
        self.assertIn("tool_id = 'object_storage'", combined)
        self.assertIn("tool_id = 'web_search'", combined)
        self.assertIn("tool_id = 'internal_rest_api'", combined)


if __name__ == "__main__":
    unittest.main()
