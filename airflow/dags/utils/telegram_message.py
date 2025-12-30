import os
from typing import Any, Tuple

import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from utils.constants import VAR_TG_NAME
from utils.dbt_logger import log


def _get_telegram_creds() -> Tuple[str | None, str | None]:

    try:
        cfg = Variable.get("telegram_config", default_var=None, deserialize_json=True)
        if isinstance(cfg, dict):
            token = str(cfg.get("token")) if cfg.get("token") else None
            chat_id = str(cfg.get("chat_id")) if cfg.get("chat_id") else None
            if token and chat_id:
                return token, chat_id
    except Exception as e:

        log.warning(f"Telegram Variable 'telegram_config' not found/invalid: {e}")


    try:
        conn = BaseHook.get_connection(VAR_TG_NAME)
        token = conn.password
        chat_id = conn.host
        if token and chat_id:
            return token, chat_id
    except Exception as e:
        log.error(
            f"Telegram connection '{VAR_TG_NAME}' not found: {e}")


    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        return token, chat_id

    return None, None


def send_telegram_message(text: str) -> None:
    token, chat_id = _get_telegram_creds()
    if not token or not chat_id:
        log.warning("Telegram is not configured; skipping notification")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}

    try:
        r = requests.post(url, json=payload, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log.error(f"Failed to send Telegram message: {e}")


def on_failure_callback(context: Any) -> None:
    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", "unknown_dag")
    task_id = getattr(task_instance, "task_id", "unknown_task")
    log_url = getattr(task_instance, "log_url", "")

    send_telegram_message(
        f"❌ Airflow task failed\nDAG: {dag_id}\nTask: {task_id}\nLogs: {log_url}"
    )


def on_success_callback(context: Any) -> None:
    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", "unknown_dag")
    run_id = getattr(dag_run, "run_id", "unknown_run")

    send_telegram_message(
        f"✅ Airflow DAG succeeded\nDAG: {dag_id}\nRun: {run_id}"
    )
