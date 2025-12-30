from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Iterable

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup

from utils.constants import CONN_NAME, DBT_ROOT_PATH
from utils.dbt_logger import (
    log_dag_success_callback,
    log_failure_callback,
    log_start_callback,
    log_success_callback,
)
from utils.dbt_runner import run_dbt
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


def _dbt_args(
    command: str,
    select: str | None = None,
    full_refresh: bool = False,
    extra_args: Iterable[str] | None = None,
) -> list[str]:
    args: list[str] = [command]

    if select:
        args += ["--select", select]

    if full_refresh:
        args.append("--full-refresh")

    if extra_args:
        args += list(extra_args)

    args += ["--project-dir", str(DBT_ROOT_PATH), "--profiles-dir", str(DBT_ROOT_PATH)]
    return args


def dbt_deps() -> None:
    run_dbt(_dbt_args("deps"))


def dbt_seed(full_refresh: bool = False) -> None:
    run_dbt(_dbt_args("seed", select="tag:seed", full_refresh=full_refresh))


def dbt_run_tag(tag: str, full_refresh: bool = False) -> None:
    run_dbt(_dbt_args("run", select=f"tag:{tag}", full_refresh=full_refresh))


def choose_load_mode() -> str:
    """Return the task_id to follow: 'initial_load' or 'incremental_load'.

    Logic:
      - If RAW_VAULT.HUB_CUSTOMER doesn't exist OR has 0 rows -> initial load
      - Else -> incremental load
    """
    cfg = get_snowflake_config()
    database = cfg.get("SNOWFLAKE_DATABASE")
    if not database:
        return "initial_load"

    hook = SnowflakeHook(snowflake_conn_id=CONN_NAME)

    try:
        result = hook.get_first(
            f"SELECT COUNT(*) FROM {database}.RAW_VAULT.HUB_CUSTOMER"
        )
        row_count = int(result[0]) if result and result[0] is not None else 0
        return "incremental_load" if row_count > 0 else "initial_load"
    except Exception:

        return "initial_load"


with DAG(
    dag_id="snowflake_data_vault_modular",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    on_success_callback=dag_success_handler,
    tags=["dbt", "snowflake", "vault"],
) as dag:

    start = EmptyOperator(task_id="start")

    choose_mode = BranchPythonOperator(
        task_id="choose_load_mode",
        python_callable=choose_load_mode,
    )

    initial = EmptyOperator(task_id="initial_load")
    incremental = EmptyOperator(task_id="incremental_load")

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")


    with TaskGroup(group_id="initial_pipeline") as initial_pipeline:
        deps = PythonOperator(task_id="dbt_deps", python_callable=dbt_deps)
        seeds = PythonOperator(
            task_id="dbt_seed_full_refresh",
            python_callable=lambda: dbt_seed(full_refresh=True),
        )
        staging = PythonOperator(
            task_id="staging",
            python_callable=lambda: dbt_run_tag("staging", full_refresh=False),
        )
        raw_vault = PythonOperator(
            task_id="raw_vault",
            python_callable=lambda: dbt_run_tag("raw_vault", full_refresh=True),
        )
        business_vault = PythonOperator(
            task_id="business_vault",
            python_callable=lambda: dbt_run_tag("business_vault", full_refresh=True),
        )
        marts = PythonOperator(
            task_id="marts",
            python_callable=lambda: dbt_run_tag("marts", full_refresh=True),
        )

        deps >> seeds >> staging >> raw_vault >> business_vault >> marts


    with TaskGroup(group_id="incremental_pipeline") as incremental_pipeline:
        deps = PythonOperator(task_id="dbt_deps", python_callable=dbt_deps)
        seeds = PythonOperator(
            task_id="dbt_seed",
            python_callable=lambda: dbt_seed(full_refresh=False),
        )
        staging = PythonOperator(
            task_id="staging",
            python_callable=lambda: dbt_run_tag("staging", full_refresh=False),
        )
        raw_vault = PythonOperator(
            task_id="raw_vault",
            python_callable=lambda: dbt_run_tag("raw_vault", full_refresh=False),
        )
        business_vault = PythonOperator(
            task_id="business_vault",
            python_callable=lambda: dbt_run_tag("business_vault", full_refresh=False),
        )
        marts = PythonOperator(
            task_id="marts",
            python_callable=lambda: dbt_run_tag("marts", full_refresh=False),
        )

        deps >> seeds >> staging >> raw_vault >> business_vault >> marts


    start >> choose_mode
    choose_mode >> initial >> initial_pipeline >> end
    choose_mode >> incremental >> incremental_pipeline >> end

