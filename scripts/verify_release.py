from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _run_step(
    *,
    name: str,
    cmd: list[str],
    cwd: Path,
    env: dict[str, str],
    log_path: Path,
) -> dict[str, Any]:
    started = time.monotonic()
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    elapsed = round(time.monotonic() - started, 3)
    log_path.write_text(proc.stdout or "", encoding="utf-8", errors="replace")
    return {
        "name": name,
        "command": cmd,
        "exit_code": int(proc.returncode),
        "duration_sec": elapsed,
        "log": str(log_path).replace("\\", "/"),
    }


def _wait_http_ready(url: str, timeout_s: int, log_path: Path) -> dict[str, Any]:
    started = time.monotonic()
    deadline = time.monotonic() + timeout_s
    last_error = "not started"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310 - fixed internal URL from CLI args
                code = int(getattr(resp, "status", 200))
                if 200 <= code < 400:
                    elapsed = round(time.monotonic() - started, 3)
                    log_path.write_text(f"ready url={url} status={code}\n", encoding="utf-8")
                    return {
                        "name": f"wait:{url}",
                        "exit_code": 0,
                        "duration_sec": elapsed,
                        "log": str(log_path).replace("\\", "/"),
                    }
                last_error = f"status={code}"
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = str(exc)
        time.sleep(2)

    elapsed = round(time.monotonic() - started, 3)
    log_path.write_text(f"timeout url={url} last_error={last_error}\n", encoding="utf-8")
    return {
        "name": f"wait:{url}",
        "exit_code": 1,
        "duration_sec": elapsed,
        "log": str(log_path).replace("\\", "/"),
    }


def _build_summary_txt(summary: dict[str, Any]) -> str:
    lines = [
        f"verification_dir={summary['verification_dir']}",
        f"generated_at={summary['generated_at']}",
        f"all_passed={str(summary['all_passed']).lower()}",
    ]
    for step in summary["steps"]:
        lines.append(f"{step['name']}_exit={step['exit_code']}")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run reproducible integration/eval verification and emit summary artifacts.")
    parser.add_argument("--base-url", default="http://localhost:18000")
    parser.add_argument("--prom-url", default="http://localhost:9090")
    parser.add_argument("--cases", default="eval/golden_cases.yaml")
    parser.add_argument("--summary-dir", default="")
    parser.add_argument("--wait-timeout-s", type=int, default=180)
    parser.add_argument("--skip-docker-up", action="store_true")
    parser.add_argument("--skip-docker-down", action="store_true")
    parser.add_argument("--skip-seed", action="store_true")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    summary_dir = Path(args.summary_dir) if args.summary_dir else root / "artifacts" / "verification" / _utc_tag()
    summary_dir.mkdir(parents=True, exist_ok=True)
    latest_file = root / "artifacts" / "verification" / "LATEST.txt"
    latest_file.parent.mkdir(parents=True, exist_ok=True)

    steps: list[dict[str, Any]] = []

    def _run(name: str, cmd: list[str], *, env_overrides: dict[str, str] | None = None) -> int:
        env = os.environ.copy()
        if env_overrides:
            env.update(env_overrides)
        step = _run_step(
            name=name,
            cmd=cmd,
            cwd=root,
            env=env,
            log_path=summary_dir / f"{name}.log",
        )
        steps.append(step)
        return int(step["exit_code"])

    if not args.skip_docker_up:
        _run(
            "docker_up",
            [
                "docker",
                "compose",
                "up",
                "-d",
                "--build",
                "postgres",
                "temporal",
                "temporal-ui",
                "otel-collector",
                "fake-internal-service",
                "api",
                "worker",
                "prometheus",
            ],
        )

    api_ready_url = f"{args.base_url.rstrip('/')}/healthz"
    prom_ready_url = f"{args.prom_url.rstrip('/')}/-/ready"
    steps.append(_wait_http_ready(api_ready_url, timeout_s=args.wait_timeout_s, log_path=summary_dir / "wait_api.log"))
    steps.append(_wait_http_ready(prom_ready_url, timeout_s=args.wait_timeout_s, log_path=summary_dir / "wait_prometheus.log"))

    if not args.skip_seed:
        _run("seed", ["docker", "compose", "exec", "-T", "api", "python", "-m", "app.seed"])

    _run(
        "pytest_integration",
        [sys.executable, "scripts/test.py", "--integration", "-q", "-m", "integration"],
        env_overrides={
            "RUN_INTEGRATION": "1",
            "INTEGRATION_REQUIRE_SIGNAL_FAIL_ONCE": "1",
            "INTEGRATION_EXPECT_RERUN_409": "1",
        },
    )
    _run(
        "eval",
        [sys.executable, "scripts/eval.py", "--base-url", args.base_url, "--cases", args.cases],
    )
    _run(
        "check_rerun_plan_hash",
        [sys.executable, "eval/check_rerun_plan_hash.py", "--base-url", args.base_url],
    )
    _run(
        "check_cost_metrics",
        [sys.executable, "eval/check_cost_metrics.py", "--base-url", args.base_url, "--prom-url", args.prom_url],
    )

    if not args.skip_docker_down:
        _run("docker_down", ["docker", "compose", "down", "-v"])

    required_step_names = {
        f"wait:{api_ready_url}",
        f"wait:{prom_ready_url}",
        "pytest_integration",
        "eval",
        "check_rerun_plan_hash",
        "check_cost_metrics",
    }
    if not args.skip_seed:
        required_step_names.add("seed")
    if not args.skip_docker_up:
        required_step_names.add("docker_up")

    failed_required = [s for s in steps if s["name"] in required_step_names and int(s["exit_code"]) != 0]
    all_passed = len(failed_required) == 0

    summary = {
        "verification_dir": str(summary_dir).replace("\\", "/"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "all_passed": all_passed,
        "steps": steps,
    }
    (summary_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=True, indent=2), encoding="utf-8")
    (summary_dir / "summary.txt").write_text(_build_summary_txt(summary), encoding="utf-8")
    latest_file.write_text(str(summary_dir).replace("\\", "/") + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=True, indent=2))

    return 0 if all_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
