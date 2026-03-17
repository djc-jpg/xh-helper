import tempfile
import unittest
from pathlib import Path

from app.services.retrieval_service import RetrievalService


class RetrievalServiceTests(unittest.TestCase):
    def test_retrieval_hits_local_docs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "approval.md").write_text(
                "# Approval Outbox\nThe approval signal outbox guarantees reliable signal delivery.\n",
                encoding="utf-8",
            )
            (base / "other.md").write_text(
                "# Metrics\nThis file talks about dashboards.\n",
                encoding="utf-8",
            )
            service = RetrievalService(docs_dir=str(base))
            hits = service.retrieve(query="approval signal outbox", top_k=3)

        self.assertTrue(hits)
        self.assertEqual("approval", hits[0]["title"])
        self.assertIn("approval", " ".join(hits[0]["matched_terms"]))


if __name__ == "__main__":
    unittest.main()
