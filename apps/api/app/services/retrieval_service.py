from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from ..config import settings

TOKEN_PATTERN = re.compile(r"[a-z0-9_]{2,}")


def _tokenize(text: str) -> list[str]:
    return TOKEN_PATTERN.findall(text.lower())


def _snippet(text: str, query_tokens: list[str], max_len: int = 200) -> str:
    lowered = text.lower()
    idx = -1
    for token in query_tokens:
        idx = lowered.find(token)
        if idx >= 0:
            break
    if idx < 0:
        return " ".join(text.split())[:max_len]
    start = max(0, idx - 60)
    end = min(len(text), idx + max_len)
    return " ".join(text[start:end].split())


class RetrievalService:
    def __init__(self, docs_dir: str | None = None) -> None:
        self._docs_dir = Path(docs_dir or settings.docs_dir)

    def retrieve(self, *, query: str, top_k: int = 3) -> list[dict[str, Any]]:
        query_tokens = _tokenize(query)
        if not query_tokens:
            return []
        if not self._docs_dir.exists():
            return []

        results: list[dict[str, Any]] = []
        for path in sorted(self._docs_dir.glob("**/*")):
            if not path.is_file() or path.suffix.lower() not in {".md", ".txt"}:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except Exception:
                continue
            doc_tokens = _tokenize(text)
            if not doc_tokens:
                continue
            token_set = set(doc_tokens)
            overlap = [token for token in query_tokens if token in token_set]
            if not overlap:
                continue
            score = len(overlap) / max(1, len(set(query_tokens)))
            results.append(
                {
                    "source": str(path),
                    "title": path.stem,
                    "score": round(score, 4),
                    "snippet": _snippet(text, overlap),
                    "matched_terms": overlap,
                }
            )
        results.sort(key=lambda item: float(item["score"]), reverse=True)
        return results[: max(1, int(top_k))]
