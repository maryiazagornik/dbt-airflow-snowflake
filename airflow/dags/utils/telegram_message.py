import requests
from typing import Any
from airflow.hooks.base import BaseHook
from utils.constants import VAR_TG_NAME
from utils.dbt_logger import log


def _get_telegram_creds():
    try:
        conn = BaseHook.get_connection(VAR_TG_NAME)
        return conn.password, conn.host
    except Exception as e:
        log.error(f"Telegram connection '{VAR_TG_NAME}' not found: {e}")
        return None, None


def send_telegram_message(context: dict[str, Any], status: str = "failure"):
    token, chat_id = _get_telegram_creds()
    if not token or not chat_id:
        return

    ti = context.get("task_instance")
    emoji = "❌" if status == "failure" else "✅"
    log_url = ti.log_url

    message = (
        f"{emoji} <b>{status.upper()}</b>\n"
        f"<b>DAG:</b> {ti.dag_id}\n"
        f"<b>Task:</b> {ti.task_id}\n"
        f"<a href='{log_url}'>🔍 View Logs</a>"
    )

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(
            url,
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram failed: {e}")


def on_failure_callback(context):
    send_telegram_message(context, status="failure")


def on_success_callback(context):
    send_telegram_message(context, status="success")
