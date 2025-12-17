from datetime import datetime, timedelta
from airflow import DAG
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from utils.constants import DBT_ROOT_PATH, PROFILES_FILEPATH
from utils.get_creds import get_snowflake_config
from utils.telegram_message import on_failure_callback, on_success_callback

dbt_env = get_snowflake_config()

profile_config = ProfileConfig(
    profile_name="snowflake_analytics",
    target_name="dev",
    profiles_yml_filepath=PROFILES_FILEPATH,
)

execution_config = ExecutionConfig(
    dbt_executable_path="dbt",
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="snowflake_data_vault_modular",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    on_success_callback=on_success_callback,
    tags=["dbt", "snowflake", "vault"],
) as dag:
    staging_tg = DbtTaskGroup(
        group_id="staging",
        project_config=ProjectConfig(DBT_ROOT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["tag:staging"]),
        operator_args={"env": dbt_env},
    )

    raw_vault_tg = DbtTaskGroup(
        group_id="raw_vault",
        project_config=ProjectConfig(DBT_ROOT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["tag:raw_vault"]),
        operator_args={"env": dbt_env},
    )

    business_vault_tg = DbtTaskGroup(
        group_id="business_vault",
        project_config=ProjectConfig(DBT_ROOT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["tag:business_vault"]),
        operator_args={"env": dbt_env},
    )

    marts_tg = DbtTaskGroup(
        group_id="marts",
        project_config=ProjectConfig(DBT_ROOT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["tag:marts"]),
        operator_args={"env": dbt_env},
    )

    staging_tg >> raw_vault_tg >> business_vault_tg >> marts_tg
