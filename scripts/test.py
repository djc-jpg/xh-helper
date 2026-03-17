from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Run repository tests without Make.")
    parser.add_argument(
        "--integration",
        action="store_true",
        help="Enable integration tests (requires external services).",
    )
    args, passthrough = parser.parse_known_args()

    cmd = [sys.executable, "-m", "pytest"]
    if passthrough:
        cmd.extend(passthrough)
    else:
        cmd.append("-q")

    env = os.environ.copy()
    worker_src = str(root / "apps" / "worker")
    existing_pythonpath = env.get("PYTHONPATH", "")
    if existing_pythonpath:
        env["PYTHONPATH"] = f"{worker_src}{os.pathsep}{existing_pythonpath}"
    else:
        env["PYTHONPATH"] = worker_src
    if args.integration:
        env["RUN_INTEGRATION"] = "1"

    return subprocess.call(cmd, env=env, cwd=str(root))


if __name__ == "__main__":
    raise SystemExit(main())
