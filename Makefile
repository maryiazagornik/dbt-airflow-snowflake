.PHONY: up down restart logs lint fix dbt-docs


up:
	docker-compose up -d

down:
	docker-compose down

restart: down up


logs:
	docker-compose logs -f airflow

lint: 
	uv run sqlfluff lint dbt_project/models --dialect snowflake --exclude-rules CP01,CP02,ST06,LT05

fix: 
	uv run sqlfluff fix dbt_project/models --dialect snowflake --exclude-rules CP01,CP02,ST06,LT05 --force

dbt-check:
	cd dbt_project && uv run dbt parse


help: 
	@echo "Usage: make [target]"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'