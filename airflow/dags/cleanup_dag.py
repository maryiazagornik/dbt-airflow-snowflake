from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from utils.constants import CONN_NAME
from utils.dbt_logger import (
    log_dag_success_callback,
    log_failure_callback,
    log_start_callback,
    log_success_callback,
)
from utils.get_creds import get_snowflake_config
from utils.telegram_message import on_failure_callback, on_success_callback


def failure_handler(context):
    log_failure_callback(context)
    on_failure_callback(context)


def success_handler(context):
    log_success_callback(context)
    on_success_callback(context)


def dag_success_handler(context):
    log_dag_success_callback(context)
    on_success_callback(context)


default_args: dict[str, Any] = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_execute_callback": log_start_callback,
    "on_success_callback": success_handler,
    "on_failure_callback": failure_handler,
}


def cleanup_schemas() -> None:
    """Drop & recreate DV schemas.

    This is intended for DEV/demo usage only.
    """
    cfg = get_snowflake_config()
    database = cfg.get("SNOWFLAKE_DATABASE")
    if not database:
        raise ValueError("SNOWFLAKE_DATABASE is not set")

    hook = SnowflakeHook(snowflake_conn_id=CONN_NAME)
    schemas = ["STAGING", "RAW_VAULT", "BUSINESS_VAULT", "MARTS"]

    for schema in schemas:
        hook.run(f"DROP SCHEMA IF EXISTS {database}.{schema} CASCADE")
        hook.run(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")


with DAG(
    dag_id="cleanup_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    on_success_callback=dag_success_handler,
    tags=["maintenance", "snowflake"],
) as dag:

    cleanup = PythonOperator(
        task_id="cleanup_schemas",
        python_callable=cleanup_schemas,
    )

    cleanup
