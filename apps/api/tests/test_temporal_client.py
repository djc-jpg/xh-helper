import unittest
from datetime import timedelta

from app.temporal_client import _workflow_execution_timeout


class TemporalClientTests(unittest.TestCase):
    def test_workflow_execution_timeout_adds_grace_over_ttl(self) -> None:
        timeout = _workflow_execution_timeout({"global_ttl_sec": 600})
        self.assertEqual(timedelta(seconds=900), timeout)

    def test_workflow_execution_timeout_respects_longer_ttl(self) -> None:
        timeout = _workflow_execution_timeout({"global_ttl_sec": 1800})
        self.assertEqual(timedelta(seconds=2100), timeout)
