# NovaSupport — developer shortcuts
# Requires: Docker Desktop, GNU Make (Git Bash on Windows)

COMPOSE = docker compose
APP_SERVICES = api orchestrator worker mcp-server

.PHONY: help up down restart build logs ps \
        infra infra-down \
        test test-unit test-integration eval smoke lint \
        shell-api db-shell redis-cli \
        topics seed clean

# ── Default ────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  NovaSupport — available targets"
	@echo ""
	@echo "  Full stack"
	@echo "    make up           Start all services (build if needed)"
	@echo "    make down         Stop and remove containers"
	@echo "    make restart      Rebuild app images and restart"
	@echo "    make build        Build / rebuild Docker images"
	@echo "    make logs         Tail logs for app services"
	@echo "    make ps           Show running containers"
	@echo ""
	@echo "  Infrastructure only (run apps on host)"
	@echo "    make infra        Start redpanda, postgres, redis, qdrant"
	@echo "    make infra-down   Stop infrastructure containers"
	@echo ""
	@echo "  Development"
	@echo "    make test         Run unit test suite (no infra needed)"
	@echo "    make eval         Run LLM quality evaluation (needs infra)"
	@echo "    make smoke        Run HTTP smoke tests (needs all services up)"
	@echo "    make lint         Run ruff linter"
	@echo "    make topics       Create Kafka topics (needs infra running)"
	@echo "    make seed         Seed DB with sample data"
	@echo ""
	@echo "  Debugging"
	@echo "    make shell-api    Shell inside the API container"
	@echo "    make db-shell     psql shell inside postgres"
	@echo "    make redis-cli    redis-cli inside redis"
	@echo "    make clean        Remove volumes (WARNING: deletes all data)"
	@echo ""

# ── Full stack ─────────────────────────────────────────────────────────────────

up:
	$(COMPOSE) up -d --build
	@echo ""
	@echo "  Services started:"
	@echo "    API        http://localhost:8000"
	@echo "    Kafka UI   http://localhost:8080"
	@echo "    Prometheus http://localhost:9090"
	@echo "    Grafana    http://localhost:3000  (admin / novasupport)"
	@echo ""

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) build $(APP_SERVICES)
	$(COMPOSE) up -d $(APP_SERVICES)

build:
	$(COMPOSE) build

logs:
	$(COMPOSE) logs -f $(APP_SERVICES)

ps:
	$(COMPOSE) ps

# ── Infrastructure only ────────────────────────────────────────────────────────

infra:
	$(COMPOSE) up -d redpanda postgres redis qdrant
	@echo "Infrastructure ready. Run apps locally with:"
	@echo "  KAFKA_BOOTSTRAP_SERVERS=localhost:9094 python -m apps.api.main"

infra-down:
	$(COMPOSE) stop redpanda kafka-ui postgres redis qdrant
	$(COMPOSE) rm -f redpanda kafka-ui postgres redis qdrant

# ── Development ────────────────────────────────────────────────────────────────

test:
	python -m pytest tests/unit/ -v --tb=short -q

test-unit: test

test-integration:
	python -m pytest tests/integration/ -v --tb=short -m integration

eval:
	python -m pytest tests/evaluation/test_quality.py -v --tb=short -m "evaluation and not db"

eval-full:
	python -m pytest tests/evaluation/test_quality.py -v --tb=short -m "evaluation"

smoke:
	python -m pytest tests/evaluation/test_api_smoke.py -v --tb=short -m smoke

lint:
	python -m ruff check .

topics:
	python infrastructure/kafka/create_topics.py

seed:
	python scripts/seed_database.py

# ── Debugging ──────────────────────────────────────────────────────────────────

shell-api:
	$(COMPOSE) exec api /bin/bash

db-shell:
	$(COMPOSE) exec postgres psql -U novasupport -d novasupport

redis-cli:
	$(COMPOSE) exec redis redis-cli

# ── Cleanup ────────────────────────────────────────────────────────────────────

clean:
	@echo "WARNING: This will delete all Docker volumes (database, kafka, redis data)."
	@echo "Press Ctrl+C to cancel, or Enter to continue."
	@read _
	$(COMPOSE) down -v
