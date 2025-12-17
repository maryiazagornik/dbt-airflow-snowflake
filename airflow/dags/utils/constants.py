from pathlib import Path

DBT_ROOT_PATH: Path = Path("/opt/airflow/dbt_project")
PROFILES_FILEPATH: Path = DBT_ROOT_PATH / "profiles.yml"

LOG_FILE_PATH: Path = Path("/opt/airflow/logs/dbt_pipeline.log")

CONN_NAME: str = "snowflake_config"
VAR_TG_NAME: str = "telegram_default"
