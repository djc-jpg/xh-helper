from __future__ import annotations

from pathlib import Path


def _read_lines(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _load_requirement_names(path: Path) -> set[str]:
    names: set[str] = set()
    for line in _read_lines(path):
        if line.startswith("#") or line.startswith("-r "):
            continue
        names.add(line.split("==", 1)[0].lower())
    return names


def test_requirements_dev_includes_base_requirements() -> None:
    root = Path(__file__).resolve().parents[3]
    req_dev = _read_lines(root / "requirements-dev.txt")
    assert "-r requirements.txt" in req_dev


def test_base_requirements_cover_runtime_imports() -> None:
    root = Path(__file__).resolve().parents[3]
    api_reqs = _load_requirement_names(root / "apps" / "api" / "requirements.txt")
    worker_reqs = _load_requirement_names(root / "apps" / "worker" / "requirements.txt")
    merged = api_reqs | worker_reqs

    expected = {
        "fastapi",
        "uvicorn[standard]",
        "pydantic",
        "psycopg[binary]",
        "temporalio",
        "opentelemetry-api",
        "opentelemetry-sdk",
        "opentelemetry-exporter-otlp",
        "opentelemetry-instrumentation-fastapi",
        "opentelemetry-instrumentation-httpx",
        "passlib[argon2]",
        "prometheus-client",
    }
    missing = sorted(x for x in expected if x not in merged)
    assert not missing, f"runtime requirements missing: {missing}"
