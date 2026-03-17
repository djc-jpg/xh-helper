import sys
import types
import unittest
from unittest.mock import patch

from apps.worker import graph as worker_graph


def _failing_postgres_module() -> types.ModuleType:
    module = types.ModuleType("langgraph.checkpoint.postgres")

    class _FailingPostgresSaver:
        @staticmethod
        def from_conn_string(_dsn: str):
            raise RuntimeError("forced checkpoint backend failure")

    module.PostgresSaver = _FailingPostgresSaver
    return module


class CheckpointObservabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        worker_graph.close_graph_resources()
        worker_graph.checkpoint_degraded.set(0)

    def tearDown(self) -> None:
        worker_graph.close_graph_resources()

    def test_checkpoint_init_failure_logs_and_falls_back_when_fail_fast_disabled(self) -> None:
        before = float(worker_graph.checkpoint_error_total._value.get())
        with (
            patch.object(worker_graph.settings, "langgraph_postgres_dsn", "postgresql://broken"),
            patch.object(worker_graph.settings, "langgraph_checkpoint_fail_fast", False),
            patch.dict(sys.modules, {"langgraph.checkpoint.postgres": _failing_postgres_module()}),
        ):
            with self.assertLogs("apps.worker.graph", level="ERROR") as logs:
                checkpointer = worker_graph._get_checkpointer()

        self.assertIn("MemorySaver", type(checkpointer).__name__)
        self.assertEqual(1.0, float(worker_graph.checkpoint_degraded._value.get()))
        self.assertGreater(float(worker_graph.checkpoint_error_total._value.get()), before)
        self.assertIn("langgraph_checkpoint_init_failed", "\n".join(logs.output))

    def test_checkpoint_init_failure_raises_when_fail_fast_enabled(self) -> None:
        with (
            patch.object(worker_graph.settings, "langgraph_postgres_dsn", "postgresql://broken"),
            patch.object(worker_graph.settings, "langgraph_checkpoint_fail_fast", True),
            patch.dict(sys.modules, {"langgraph.checkpoint.postgres": _failing_postgres_module()}),
        ):
            with self.assertLogs("apps.worker.graph", level="ERROR"):
                with self.assertRaises(RuntimeError):
                    worker_graph._get_checkpointer()

        self.assertEqual(1.0, float(worker_graph.checkpoint_degraded._value.get()))

    def test_checkpoint_runtime_error_logs_and_retries_when_fail_fast_disabled(self) -> None:
        before = float(worker_graph.checkpoint_error_total._value.get())
        with (
            patch.object(worker_graph.settings, "langgraph_checkpoint_fail_fast", False),
            patch("apps.worker.graph._invoke_graph", side_effect=[RuntimeError("connection is closed"), {"ok": True}]),
        ):
            with self.assertLogs("apps.worker.graph", level="ERROR") as logs:
                result = worker_graph.run_langgraph("rag_qa", {"question": "q"}, "thread-1")

        self.assertEqual({"ok": True}, result)
        self.assertEqual(1.0, float(worker_graph.checkpoint_degraded._value.get()))
        self.assertGreater(float(worker_graph.checkpoint_error_total._value.get()), before)
        self.assertIn("langgraph_checkpoint_runtime_error", "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
