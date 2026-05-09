# Enterprise AI Workflow Platform — local infra control plane.
# Targets are kept thin so the underlying compose / scripts remain inspectable.

SHELL        := /usr/bin/env bash
.SHELLFLAGS  := -eu -o pipefail -c
COMPOSE      := docker compose
PROJECT      := eaip
PG_USER      ?= eaip
PG_DB        ?= eaip

.DEFAULT_GOAL := help

# --- meta -----------------------------------------------------------------

.PHONY: help
help: ## print this help
	@awk 'BEGIN {FS = ":.*?## "}; \
	      /^[a-zA-Z_%-]+:.*?##/ { printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 }' \
	  $(MAKEFILE_LIST)

# --- lifecycle ------------------------------------------------------------

.PHONY: up
up: ## boot the stack and wait for healthchecks
	@echo "→ booting infrastructure plane"
	@$(COMPOSE) up -d
	@echo "→ waiting for healthchecks (max 120s)"
	@bash scripts/healthcheck.sh --wait 120

.PHONY: down
down: ## stop the stack but preserve volumes
	@$(COMPOSE) down

.PHONY: nuke
nuke: ## stop the stack AND destroy volumes (prompts for confirmation)
	@read -r -p "This destroys all local data (postgres, kafka, minio, grafana). Continue? [y/N] " ans; \
	  if [[ "$$ans" =~ ^[Yy]$$ ]]; then \
	    $(COMPOSE) down -v; \
	    echo "✓ volumes destroyed"; \
	  else \
	    echo "aborted"; \
	  fi

.PHONY: ps
ps: ## list services with health and ports
	@$(COMPOSE) ps --format "table {{.Service}}\t{{.Status}}\t{{.Ports}}"

# --- logs -----------------------------------------------------------------

.PHONY: logs
logs: ## tail all logs (Ctrl-C to exit)
	@$(COMPOSE) logs -f --tail=100

.PHONY: logs-%
logs-%: ## tail logs for one service, e.g. make logs-postgres
	@$(COMPOSE) logs -f --tail=200 $*

# --- health & topics ------------------------------------------------------

.PHONY: healthcheck
healthcheck: ## probe every service and print OK/FAIL summary
	@bash scripts/healthcheck.sh

.PHONY: topics
topics: ## create the v1 Kafka topics + DLQs
	@bash scripts/seed-kafka-topics.sh

# --- shells ---------------------------------------------------------------

.PHONY: psql
psql: ## open a psql shell into the postgres container
	@$(COMPOSE) exec -e PGPASSWORD=$${POSTGRES_PASSWORD:-changeme_local_only} postgres \
	  psql -U $(PG_USER) -d $(PG_DB)

.PHONY: redis-cli
redis-cli: ## open a redis-cli shell into the redis container
	@$(COMPOSE) exec redis sh -c 'redis-cli -a "$$REDIS_PASSWORD"' \
	  || $(COMPOSE) exec -e REDIS_PASSWORD=$${REDIS_PASSWORD:-changeme_local_only} redis \
	     sh -c 'redis-cli -a "$$REDIS_PASSWORD"'

.PHONY: rpk
rpk: ## open an rpk shell into the redpanda container (e.g. make rpk -- topic list)
	@$(COMPOSE) exec redpanda rpk $(filter-out $@,$(MAKECMDGOALS))

# --- API service (Phase 2) ------------------------------------------------

.PHONY: api-dev
api-dev: ## run the API locally with hot-reload (requires uv)
	@cd apps/api && uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

.PHONY: api-test
api-test: ## run the api test suite (testcontainers)
	@cd apps/api && uv run pytest -v

.PHONY: api-lint
api-lint: ## ruff check + format check on apps/api
	@cd apps/api && uv run ruff check . && uv run ruff format --check .

.PHONY: api-typecheck
api-typecheck: ## mypy strict on apps/api/app
	@cd apps/api && uv run mypy app/

.PHONY: api-migrate
api-migrate: ## alembic upgrade head against the running stack
	@cd apps/api && uv run alembic upgrade head

.PHONY: api-shell
api-shell: ## open an interactive python shell with app context loaded
	@cd apps/api && uv run python -i -c "from app.main import create_app; app=create_app(); print('app ready')"

.PHONY: api-seed
api-seed: ## seed a demo org/user (requires EAIP_SEED_DEMO=1)
	@cd apps/api && EAIP_SEED_DEMO=1 uv run python -m app.scripts.seed

# --- convenience aliases --------------------------------------------------

.PHONY: clean
clean: down ## alias for `down`

.PHONY: restart
restart: down up ## bounce the stack

# Swallow extra args so `make rpk -- topic list` does not cause make to look for "topic" as a target.
%:
	@:
