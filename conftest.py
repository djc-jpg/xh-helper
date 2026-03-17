from __future__ import annotations

import importlib
import os
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent

# Test-only import convenience:
# add repo root so pytest can resolve package-style imports without
# requiring developers to set PYTHONPATH manually in local/CI runs.
ROOT_STR = str(ROOT)
if ROOT_STR not in sys.path:
    sys.path.insert(0, ROOT_STR)

# Worker uses top-level module imports (e.g. `import mas`) in runtime mode.
# Add worker source root in tests so direct `python -m pytest ...` also works.
WORKER_SRC_STR = str(ROOT / "apps" / "worker")
if WORKER_SRC_STR not in sys.path:
    sys.path.insert(0, WORKER_SRC_STR)


def _alias_module(alias: str, target: str, *, lazy: bool = False) -> None:
    if alias in sys.modules:
        return
    if not lazy:
        sys.modules[alias] = importlib.import_module(target)
        return

    proxy = types.ModuleType(alias)
    loaded_module: types.ModuleType | None = None

    def _load() -> types.ModuleType:
        nonlocal loaded_module
        if loaded_module is None:
            loaded_module = importlib.import_module(target)
            sys.modules[alias] = loaded_module
        return loaded_module

    def _getattr(name: str):
        return getattr(_load(), name)

    proxy.__getattr__ = _getattr  # type: ignore[attr-defined]
    sys.modules[alias] = proxy


# Keep backward-compatible test imports while sys.path only contains repo root.
_alias_module("app", "apps.api.app")
_alias_module("config", "apps.worker.config", lazy=True)
_alias_module("db", "apps.worker.db", lazy=True)
_alias_module("graph", "apps.worker.graph", lazy=True)
_alias_module("repositories", "apps.worker.repositories", lazy=True)
_alias_module("idempotency", "apps.worker.idempotency", lazy=True)
_alias_module("otel", "apps.worker.otel", lazy=True)
_alias_module("workflows", "apps.worker.workflows", lazy=True)
_alias_module("activities", "apps.worker.activities", lazy=True)
_alias_module("worker", "apps.worker.worker", lazy=True)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    run_integration = os.getenv("RUN_INTEGRATION", "0") == "1"
    skip_integration = pytest.mark.skip(reason="integration test disabled; set RUN_INTEGRATION=1 to enable")

    for item in items:
        node_id = item.nodeid.replace("\\", "/")
        if "/integration/" in node_id or "test_integration" in node_id:
            item.add_marker("integration")
        if not run_integration and "integration" in item.keywords:
            item.add_marker(skip_integration)
