DC = docker compose
CONTAINER = airflow-scheduler

DBT_ROOT = /opt/airflow/dbt_project
DBT_FLAGS = --project-dir $(DBT_ROOT) --profiles-dir $(DBT_ROOT)

EXEC = $(DC) exec $(CONTAINER)
RUN = $(DC) run --rm $(CONTAINER)

.PHONY: help up down restart build logs bash \
		dbt-deps dbt-seed dbt-run dbt-test dbt-full \
		dbt-docs-gen dbt-docs-serve \
		lint clean

help:
	@echo "Usage: make [command]"
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

up:
	$(DC) up -d

build:
	$(DC) build

rebuild: down build up

down:
	$(DC) down

logs:
	$(DC) logs -f $(CONTAINER)

dbt-full:
	$(EXEC) dbt build $(DBT_FLAGS)

dbt-docs-gen:
	$(EXEC) dbt docs generate $(DBT_FLAGS)

dbt-docs-serve:
	$(RUN) -p 8001:8080 --entrypoint "dbt docs serve --port 8080 --address 0.0.0.0 --no-browser $(DBT_FLAGS)" $(CONTAINER)

lint:
	uv run sqlfluff lint dbt_project/models

clean:
	$(EXEC) dbt clean $(DBT_FLAGS)
	rm -rf dbt_project/target dbt_project/dbt_packages
