DC = docker compose
CONTAINER = airflow-scheduler

DBT_ROOT = /opt/airflow/dbt_project
DBT_FLAGS = --project-dir $(DBT_ROOT) --profiles-dir $(DBT_ROOT)

EXEC = $(DC) exec $(CONTAINER)
RUN = $(DC) run --rm $(CONTAINER)

.PHONY: help up down restart build logs docker-exec docker-bash \
		dbt-deps dbt-seed dbt-run dbt-test dbt-full \
		dbt-docs-gen dbt-docs-serve \
		lint clean

help: ## Show this help
	@echo "Usage: make [command]"
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start services (detached)
	$(DC) up -d

build: ## Build images
	$(DC) build

rebuild: down build up ## Rebuild and restart

down: ## Stop services
	$(DC) down

logs: ## Tail scheduler logs
	$(DC) logs -f $(CONTAINER)

docker-exec: ## Execute a command in the running container: make docker-exec cmd="dbt --version"
	$(DC) exec $(CONTAINER) $(cmd)

docker-bash: ## Open a bash shell in the running container
	$(DC) exec $(CONTAINER) bash

dbt-deps: ## Install dbt packages
	$(EXEC) dbt deps $(DBT_FLAGS)

dbt-seed: ## Run dbt seeds
	$(EXEC) dbt seed $(DBT_FLAGS)

dbt-run: ## Run dbt models
	$(EXEC) dbt run $(DBT_FLAGS)

dbt-test: ## Run dbt tests
	$(EXEC) dbt test $(DBT_FLAGS)

dbt-full: ## dbt build (models + tests)
	$(EXEC) dbt build $(DBT_FLAGS)

dbt-docs-gen: ## Generate dbt docs
	$(EXEC) dbt docs generate $(DBT_FLAGS)

dbt-docs-serve: ## Serve dbt docs on http://localhost:8001
	$(RUN) -p 8001:8080 --entrypoint "dbt docs serve --port 8080 --address 0.0.0.0 --no-browser $(DBT_FLAGS)" $(CONTAINER)

lint: ## Lint SQL with sqlfluff
	uv run sqlfluff lint dbt_project/models

clean: ## Clean dbt artifacts
	$(EXEC) dbt clean $(DBT_FLAGS)
	rm -rf dbt_project/target dbt_project/dbt_packages
