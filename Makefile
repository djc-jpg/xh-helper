.PHONY: bootstrap up down seed backfill-replay test eval eval-agent verify-release logs demo-create demo-status demo-approve

bootstrap:
	@if [ ! -f .env ]; then cp .env.example .env; fi
	cd apps/frontend && npm ci

up:
	docker compose up -d --build

down:
	docker compose down -v

seed:
	docker compose exec -T api python -m app.seed

backfill-replay:
	docker compose exec -T api python -m app.backfill_replay_input --dry-run

test:
	PYTHONPATH=apps/api python -m pytest -q apps/api/tests
	PYTHONPATH=apps/worker python -m pytest -q apps/worker/tests
	python eval/check_cases_yaml.py eval/golden_cases.yaml
	python eval/check_cases_yaml.py eval/agent_golden_cases.yaml

eval:
	docker compose exec -T api python /workspace/eval/run_eval.py --base-url http://api:8000
	docker compose exec -T api python /workspace/eval/check_rerun_plan_hash.py --base-url http://api:8000
	docker compose exec -T api python /workspace/eval/check_cost_metrics.py --base-url http://api:8000 --prom-url http://prometheus:9090
	@docker compose logs otel-collector | grep -E "Name[[:space:]]*:[[:space:]]*(planner|retrieval|llm_call|review|approval_wait|tool_call)" >/dev/null

eval-agent:
	docker compose exec -T api python /workspace/eval/run_eval.py --base-url http://api:8000 --cases /workspace/eval/agent_golden_cases.yaml

verify-release:
	python scripts/verify_release.py --base-url http://localhost:18000 --prom-url http://localhost:9090 --cases eval/golden_cases.yaml

logs:
	docker compose logs -f api worker frontend

demo-create:
	python scripts/demo_cli.py create

demo-status:
	@if [ -z "$(TASK_ID)" ]; then echo "Usage: make demo-status TASK_ID=<task_id>"; exit 1; fi
	python scripts/demo_cli.py status --task-id "$(TASK_ID)"

demo-approve:
	python scripts/demo_cli.py approve $(if $(APPROVAL_ID),--approval-id $(APPROVAL_ID),) $(if $(TASK_ID),--task-id $(TASK_ID),)
