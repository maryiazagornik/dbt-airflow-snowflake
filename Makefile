DC = docker-compose
EXEC = $(DC) exec airflow-scheduler 
DBT_DIR = --profiles-dir /opt/airflow/dbt_project --project-dir /opt/airflow/dbt_project

.PHONY: up down restart bash dbt-run dbt-test dbt-docs lint clean

up:
	$(DC) up -d --build

down:
	$(DC) down

restart: down up

bash:
	$(EXEC) bash

dbt-run:
	$(EXEC) dbt run $(DBT_DIR)

dbt-test:
	$(EXEC) dbt test $(DBT_DIR)

dbt-docs:
	$(EXEC) dbt docs generate $(DBT_DIR)

lint:
	$(EXEC) sqlfluff lint /opt/airflow/dbt_project/models --config /opt/airflow/dbt_project/.sqlfluff

clean:
	$(EXEC) dbt clean $(DBT_DIR)