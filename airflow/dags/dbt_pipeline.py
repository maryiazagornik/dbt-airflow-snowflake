import os
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig

# Путь к папке dbt_project внутри контейнера
DBT_ROOT_PATH = Path("/opt/airflow/dbt_project")

# Функция, которая достает настройки из Airflow Variable (JSON)
def get_env():
    try:
        # Пытаемся взять переменную snowflake_config, которую ты создала в UI
        config = Variable.get("snowflake_config", deserialize_json=True)
        return {
            "SNOWFLAKE_ACCOUNT": config.get("account"),
            "SNOWFLAKE_USER": config.get("user"),
            "SNOWFLAKE_PASSWORD": config.get("password"),
            "SNOWFLAKE_ROLE": config.get("role"),
            "SNOWFLAKE_WAREHOUSE": config.get("warehouse"),
            "SNOWFLAKE_DATABASE": config.get("database"),
            "SNOWFLAKE_SCHEMA": config.get("schema"),
        }
    except Exception:
        # Если переменной нет, fallback на системные переменные
        return dict(os.environ)

# Конфигурация профиля: используем profiles.yml, а НЕ маппинг подключений
profile_config = ProfileConfig(
    profile_name="snowflake_analytics",   # Должно совпадать с именем в dbt_project.yml
    target_name="dev",                    # Должно совпадать с именем outputs в profiles.yml
    profiles_yml_filepath=DBT_ROOT_PATH / "profiles.yml", # Явный путь к файлу
)


my_dbt_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_ROOT_PATH,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="dbt",
    ),

    operator_args={
        "env": get_env(),
    },
    dag_id="snowflake_data_vault",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "snowflake", "vault"],
)
