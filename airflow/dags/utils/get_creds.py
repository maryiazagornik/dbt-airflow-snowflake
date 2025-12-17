import os
from airflow.models import Variable
from utils.dbt_logger import log


def get_snowflake_config():
    try:
        config = Variable.get("snowflake_config", deserialize_json=True)
        log.info("Credentials retrieved from Airflow Variables")
        return {
            "SNOWFLAKE_ACCOUNT": config.get("account"),
            "SNOWFLAKE_USER": config.get("user"),
            "SNOWFLAKE_PASSWORD": config.get("password"),
            "SNOWFLAKE_ROLE": config.get("role"),
            "SNOWFLAKE_WAREHOUSE": config.get("warehouse"),
            "SNOWFLAKE_DATABASE": config.get("database"),
            "SNOWFLAKE_SCHEMA": config.get("schema"),
        }
    except Exception as e:
        log.warning(f"Using environment variables: {e}")
        return dict(os.environ)
