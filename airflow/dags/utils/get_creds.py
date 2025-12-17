import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from utils.constants import CONN_NAME
from utils.dbt_logger import log


def get_snowflake_config() -> dict[str, str]:
    try:
        hook = SnowflakeHook(snowflake_conn_id=CONN_NAME)
        conn = hook.get_connection(hook.snowflake_conn_id)

        extra = conn.extra_dejson or {}

        log.info(
            f"Credentials successfully retrieved from Airflow Connection: {CONN_NAME}"
        )

        return {
            "SNOWFLAKE_ACCOUNT": conn.host or extra.get("account"),
            "SNOWFLAKE_USER": conn.login,
            "SNOWFLAKE_PASSWORD": conn.password,
            "SNOWFLAKE_ROLE": extra.get("role"),
            "SNOWFLAKE_WAREHOUSE": extra.get("warehouse"),
            "SNOWFLAKE_DATABASE": extra.get("database"),
            "SNOWFLAKE_SCHEMA": extra.get("schema") or conn.schema,
        }
    except Exception as e:
        log.warning(
            f"Could not find Airflow Connection '{CONN_NAME}': {e}. Falling back to ENV."
        )
        return dict(os.environ)
