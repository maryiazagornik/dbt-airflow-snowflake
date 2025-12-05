import os
from pathlib import Path
from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


DBT_ROOT_PATH = Path("/opt/airflow/dbt_project")

profile_config = ProfileConfig(
    profile_name="dbt_project",  
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default", 
        profile_args={
            "database": "ANALYTICS",
            "schema": "RAW_VAULT"
        },
    )
)

my_dbt_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_ROOT_PATH,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="dbt",
    ),
    dag_id="snowflake_data_vault",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "snowflake", "vault"],
)