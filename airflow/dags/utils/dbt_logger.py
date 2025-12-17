import sys
from typing import Any
from loguru import logger
from airflow.models import TaskInstance
from utils.constants import LOG_FILE_PATH

logger.remove()
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan> - <level>{message}</level>",
    level="INFO",
)
logger.add(
    LOG_FILE_PATH,
    rotation="10 MB",
    retention="10 days",
    compression="zip",
    level="DEBUG",
    serialize=True,
)

log = logger


def log_start_callback(context: dict[str, Any]) -> None:
    ti: TaskInstance = context.get("task_instance")
    log.info(f"🚀 Executing Task: {ti.task_id} | DAG: {ti.dag_id}")


def log_success_callback(context: dict[str, Any]) -> None:
    ti: TaskInstance = context.get("task_instance")
    log.success(
        f"✅ Task Success: {ti.task_id} | DAG: {ti.dag_id} | Duration: {ti.duration}s"
    )


def log_failure_callback(context: dict[str, Any]) -> None:
    ti: TaskInstance = context.get("task_instance")
    exception = context.get("exception")
    log.error(f"❌ Task Failed: {ti.task_id} | DAG: {ti.dag_id} | Error: {exception}")
