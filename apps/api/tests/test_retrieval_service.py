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

    def test_retrieval_ignores_stopword_only_overlap(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            (base / "product.md").write_text(
                "# Product\nThis platform is built in a secure way and can run workflows.\n",
                encoding="utf-8",
            )
            (base / "security.md").write_text(
                "# Security\nThis file explains what is allowed in the runtime.\n",
                encoding="utf-8",
            )
            service = RetrievalService(docs_dir=str(base))
            hits = service.retrieve(query="what can you do in this workspace", top_k=3)

        self.assertEqual([], hits)


if __name__ == "__main__":
    unittest.main()
