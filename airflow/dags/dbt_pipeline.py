import os
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)

DBT_ROOT_PATH = Path("/opt/airflow/dbt_project")


def get_env():
    try:
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
        return dict(os.environ)


profile_config = ProfileConfig(
    profile_name="snowflake_analytics",
    target_name="dev",
    profiles_yml_filepath=DBT_ROOT_PATH / "profiles.yml",
)

execution_config = ExecutionConfig(
    dbt_executable_path="dbt",
)

with DAG(
    dag_id="snowflake_data_vault_modular",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "snowflake", "vault"],
) as dag:
    dbt_env = get_env()

    staging_tg = DbtTaskGroup(
        group_id="staging",
        project_config=ProjectConfig(DBT_ROOT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["tag:staging"], env_vars=dbt_env),
        operator_args={"env": dbt_env},
    )

    raw_vault_tg = DbtTaskGroup(
        group_id="raw_vault",
        project_config=ProjectConfig(DBT_ROOT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["tag:raw_vault"], env_vars=dbt_env),
        operator_args={"env": dbt_env},
    )

    marts_tg = DbtTaskGroup(
        group_id="marts",
        project_config=ProjectConfig(DBT_ROOT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["tag:marts"], env_vars=dbt_env),
        operator_args={"env": dbt_env},
    )

    staging_tg >> raw_vault_tg >> marts_tg
