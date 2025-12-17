from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode

from utils.constants import DBT_ROOT_PATH, PROFILES_FILEPATH
from utils.dbt_logger import (
    log_dag_success_callback,
    log_failure_callback,
    log_start_callback,
    log_success_callback,
)
from utils.get_creds import get_snowflake_config
from utils.telegram_message import on_failure_callback, on_success_callback


dbt_env = get_snowflake_config()


def failure_handler(context):
    log_failure_callback(context)
    on_failure_callback(context)


def success_handler(context):
    log_success_callback(context)
    on_success_callback(context)


def dag_success_handler(context):
    log_dag_success_callback(context)
    on_success_callback(context)


profile_config = ProfileConfig(
    profile_name="snowflake_analytics",
    target_name="dev",
    profiles_yml_filepath=PROFILES_FILEPATH,
)

default_args: dict[str, Any] = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_execute_callback": log_start_callback,
    "on_success_callback": success_handler,
    "on_failure_callback": failure_handler,
}

common_operator_args = {
    "install_deps": True,
    "env": dbt_env,
}

with DAG(
    dag_id="snowflake_data_vault_modular",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    on_success_callback=dag_success_handler,
    tags=["dbt", "snowflake", "vault"],
) as dag:

    def create_dbt_group(group_id: str, tag: str) -> DbtTaskGroup:
        return DbtTaskGroup(
            group_id=group_id,
            project_config=ProjectConfig(DBT_ROOT_PATH),
            profile_config=profile_config,
            execution_config=ExecutionConfig(dbt_executable_path="dbt"),
            render_config=RenderConfig(
                select=[f"tag:{tag}"], load_method=LoadMode.DBT_LS
            ),
            operator_args=common_operator_args,
        )

    staging_tg = create_dbt_group("staging", "staging")
    raw_vault_tg = create_dbt_group("raw_vault", "raw_vault")
    business_vault_tg = create_dbt_group("business_vault", "business_vault")
    marts_tg = create_dbt_group("marts", "marts")

    staging_tg >> raw_vault_tg >> business_vault_tg >> marts_tg
