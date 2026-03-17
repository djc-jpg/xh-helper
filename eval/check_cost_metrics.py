from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any

import requests


def login(base_url: str) -> str:
    email = os.getenv("SEED_OPERATOR_EMAIL", "operator@example.com")
    password = os.getenv("SEED_OPERATOR_PASSWORD", "")
    if not password:
        raise RuntimeError("Missing SEED_OPERATOR_PASSWORD.")
    resp = requests.post(
        f"{base_url}/auth/login",
        json={"email": email, "password": password},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def parse_metric_samples(metrics_text: str, metric_name: str) -> list[float]:
    pattern = re.compile(rf"^{re.escape(metric_name)}(?:\{{.*\}})?\s+([0-9eE+\-.]+)$")
    values: list[float] = []
    for line in metrics_text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        matched = pattern.match(line)
        if matched:
            values.append(float(matched.group(1)))
    return values


def query_prometheus(prom_url: str, expr: str) -> dict[str, Any]:
    resp = requests.get(
        f"{prom_url}/api/v1/query",
        params={"query": expr},
        timeout=15,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("status") != "success":
        raise RuntimeError(f"prom query failed: {expr}")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:18000")
    parser.add_argument("--prom-url", default="http://localhost:9090")
    args = parser.parse_args()
    base_url = args.base_url.rstrip("/")
    prom_url = args.prom_url.rstrip("/")

    token = login(base_url)
    internal_token = os.getenv("INTERNAL_API_TOKEN", "")
    tasks = requests.get(f"{base_url}/tasks", headers=auth_headers(token), timeout=20)
    tasks.raise_for_status()
    task_rows = tasks.json()
    max_task_cost = max((float(x.get("cost_total") or 0) for x in task_rows), default=0.0)

    metrics_headers: dict[str, str] = {}
    if internal_token:
        metrics_headers["X-Internal-Token"] = internal_token
    metrics_resp = requests.get(f"{base_url}/metrics", headers=metrics_headers, timeout=20)
    metrics_resp.raise_for_status()
    metrics_text = metrics_resp.text
    task_cost_samples = parse_metric_samples(metrics_text, "task_cost_usd")
    task_budget_samples = parse_metric_samples(metrics_text, "task_budget_usd")

    rules_resp = requests.get(f"{prom_url}/api/v1/rules", timeout=20)
    rules_resp.raise_for_status()
    rules = rules_resp.json()
    groups = rules.get("data", {}).get("groups", [])
    has_cost_spike_rule = any(
        any(rule.get("name") == "TaskCostSpike" for rule in group.get("rules", []))
        for group in groups
    )

    prom_max_cost = query_prometheus(prom_url, "max(task_cost_usd)")
    prom_max_budget = query_prometheus(prom_url, "max(task_budget_usd)")

    result = {
        "task_rows": len(task_rows),
        "max_task_cost_db": max_task_cost,
        "metrics_task_cost_samples": len(task_cost_samples),
        "metrics_task_budget_samples": len(task_budget_samples),
        "metrics_task_cost_max": max(task_cost_samples) if task_cost_samples else 0.0,
        "has_alert_rule_task_cost_spike": has_cost_spike_rule,
        "prom_max_cost_result_size": len(prom_max_cost.get("data", {}).get("result", [])),
        "prom_max_budget_result_size": len(prom_max_budget.get("data", {}).get("result", [])),
    }
    print(json.dumps(result, ensure_ascii=True, indent=2))

    ok = True
    if max_task_cost <= 0:
        print("check_fail: max_task_cost_db <= 0", file=sys.stderr)
        ok = False
    if not task_cost_samples:
        print("check_fail: task_cost_usd metric samples missing", file=sys.stderr)
        ok = False
    if max(task_cost_samples or [0.0]) <= 0:
        print("check_fail: task_cost_usd metric max <= 0", file=sys.stderr)
        ok = False
    if not task_budget_samples:
        print("check_fail: task_budget_usd metric samples missing", file=sys.stderr)
        ok = False
    if not has_cost_spike_rule:
        print("check_fail: TaskCostSpike alert rule missing", file=sys.stderr)
        ok = False

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
