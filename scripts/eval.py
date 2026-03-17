from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    run_eval = root / "eval" / "run_eval.py"
    cmd = [sys.executable, str(run_eval), *sys.argv[1:]]
    return subprocess.call(cmd, cwd=str(root))


if __name__ == "__main__":
    raise SystemExit(main())
