DC = docker-compose
CONTAINER = airflow-scheduler
DBT_ROOT = /opt/airflow/dbt_project
DBT_FLAGS = --project-dir $(DBT_ROOT) --profiles-dir $(DBT_ROOT)

EXEC = $(DC) exec $(CONTAINER)
RUN = $(DC) run --rm

.PHONY: help up down restart logs bash \
		dbt-deps dbt-seed dbt-run dbt-test dbt-full \
		dbt-docs-gen dbt-docs-serve \
		lint clean

help:
	@echo "Available commands:"
	@echo "  make up          - Start Airflow (and rebuild)"
	@echo "  make down        - Stop all containers"
	@echo "  make restart     - Restart containers"
	@echo "  make logs        - Follow Airflow Scheduler logs"
	@echo "  make bash        - Enter the Airflow Scheduler container shell"

up:
	$(DC) up -d --build

down:
	$(DC) down

restart: down up

logs:
	$(DC) logs -f $(CONTAINER)

bash:
	$(EXEC) bash

dbt-deps:
	$(EXEC) dbt deps $(DBT_FLAGS)

dbt-seed:
	$(EXEC) dbt seed $(DBT_FLAGS) --full-refresh

dbt-run:
	$(EXEC) dbt run $(DBT_FLAGS)

dbt-test:
	$(EXEC) dbt test $(DBT_FLAGS)

dbt-full: dbt-deps dbt-seed dbt-run dbt-test

dbt-docs-gen:
	$(EXEC) dbt docs generate $(DBT_FLAGS)

dbt-docs-serve:
	$(RUN) -p 8001:8080 --entrypoint "dbt docs serve --port 8080 --address 0.0.0.0 --no-browser $(DBT_FLAGS)" $(CONTAINER)

lint:
	$(EXEC) sqlfluff lint $(DBT_ROOT)/models --config $(DBT_ROOT)/.sqlfluff

clean:
	$(EXEC) dbt clean $(DBT_FLAGS)
