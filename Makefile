.PHONY: help install dev start stop restart logs shell \
        migrate migrate-new migrate-history \
        build up down clean \
        test lint format \
        minio-up kafka-up

PYTHON   := .venv/bin/python
UV       := uv
UVICORN  := .venv/bin/uvicorn
ALEMBIC  := .venv/bin/alembic
PYTEST   := .venv/bin/pytest

# ── Help ──────────────────────────────────────────────────────────────────────
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' | sort

# ── Local dev ─────────────────────────────────────────────────────────────────
install: ## Install all dependencies via uv
	$(UV) sync

dev: ## Run FastAPI with hot-reload
	$(UVICORN) app.main:app --reload --host 0.0.0.0 --port 8002

# ── Database migrations ───────────────────────────────────────────────────────
migrate: ## Apply all pending Alembic migrations
	$(ALEMBIC) upgrade head

migrate-new: ## Create a new migration (MSG="describe change")
	$(ALEMBIC) revision --autogenerate -m "$(MSG)"

migrate-history: ## Show Alembic migration history
	$(ALEMBIC) history --verbose

migrate-down: ## Roll back the last migration
	$(ALEMBIC) downgrade -1

# ── Docker Compose ────────────────────────────────────────────────────────────
up: ## Start all infrastructure services (Kafka, Redis, Postgres, MinIO)
	docker compose up -d

down: ## Stop all infrastructure services
	docker compose down

restart: ## Restart all infrastructure services
	docker compose restart

logs: ## Tail logs for all services (CTRL-C to stop)
	docker compose logs -f

logs-%: ## Tail logs for a specific service  e.g. make logs-kafka
	docker compose logs -f $*

shell-%: ## Open a shell in a running container  e.g. make shell-kafka
	docker compose exec $* /bin/bash

# ── Build & run app container ─────────────────────────────────────────────────
build: ## Build the application Docker image
	docker build -t messaging-system:latest .

app-run: ## Run the app container (requires infrastructure to be up)
	docker run --rm --network host \
		--env-file .env \
		messaging-system:latest

# ── Tests & quality ───────────────────────────────────────────────────────────
test: ## Run the test suite
	$(PYTEST) -v

lint: ## Lint with ruff (if installed)
	$(UV) run ruff check app/

format: ## Format with ruff (if installed)
	$(UV) run ruff format app/

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean: ## Remove __pycache__ and .pyc files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete

clean-all: clean ## Remove venv and all generated artifacts
	rm -rf .venv

# ── Convenience scripts ───────────────────────────────────────────────────────
startup: ## Run the startup script (creates topics, seeds DB, etc.)
	$(PYTHON) scripts/startup.py

load-test: ## Run the load test script
	$(PYTHON) scripts/load_test.py

simulate-ws: ## Simulate WebSocket history
	$(PYTHON) scripts/simulate_ws_history.py
