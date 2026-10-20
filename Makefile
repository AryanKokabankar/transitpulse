# Linux/CI equivalent of tasks.py. Both exist because the project is developed
# on Windows (no make) and built in CI on Linux (no reason not to use make).
.DEFAULT_GOAL := help
.PHONY: help bootstrap up down nuke ps migrate topology capture replay bridge smoke partitions lint fmt typecheck test test-integration check build

PY ?= python

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

bootstrap: ## Create .venv and install dev dependencies
	$(PY) tasks.py bootstrap

up: ## Start the local stack
	$(PY) tasks.py up

down: ## Stop the local stack
	$(PY) tasks.py down

nuke: ## Stop the stack and delete volumes
	$(PY) tasks.py nuke

ps: ## Show stack status
	$(PY) tasks.py ps

migrate: ## Apply database migrations
	$(PY) tasks.py migrate

topology: ## Create Pub/Sub topics, subscription and DLQ
	$(PY) tasks.py topology

capture: ## Archive one feed snapshot into the replay corpus
	$(PY) tasks.py capture $(ARGS)

replay: ## Replay the corpus (ARGS="--rate 500 --duration 10s")
	$(PY) tasks.py replay $(ARGS)

bridge: ## Deliver published messages to the processor (ARGS="--duration 30s")
	$(PY) tasks.py bridge $(ARGS)

smoke: ## Provoke every metric the dashboard and alert rules read
	$(PY) tasks.py smoke $(ARGS)

partitions: ## Maintain vehicle_events partitions and report the headroom
	$(PY) tasks.py partitions $(ARGS)

lint: ## ruff check + format check
	$(PY) tasks.py lint

fmt: ## Auto-format and auto-fix
	$(PY) tasks.py fmt

typecheck: ## mypy --strict
	$(PY) tasks.py typecheck

test: ## Unit tests
	$(PY) tasks.py test

test-integration: ## Integration tests (needs the stack up)
	$(PY) tasks.py test-integration

check: ## lint + typecheck + test
	$(PY) tasks.py check

build: ## Build the service image
	docker build -t transitpulse:local .
