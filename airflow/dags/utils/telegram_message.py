import requests
from typing import Any
from airflow.hooks.base import BaseHook
from utils.dbt_logger import log
from utils.constants import VAR_TG_NAME


def _get_telegram_creds():
    try:
        conn = BaseHook.get_connection(VAR_TG_NAME)
        return conn.password, conn.host
    except Exception as e:
        log.error(f"Telegram connection '{VAR_TG_NAME}' not found: {e}")
        return None, None


def build_message(context: dict[str, Any], status: str, emoji: str) -> str:
    ti = context.get("task_instance")
    dag_id = ti.dag_id
    task_id = ti.task_id
    run_id = context.get("run_id")
    duration = round(ti.duration, 2) if ti.duration else "n/a"
    log_url = ti.log_url
    exception = context.get("exception")

    msg = (
        f"{emoji} <b>{status.upper()}</b>\n\n"
        f"<b>DAG:</b> {dag_id}\n"
        f"<b>Task:</b> {task_id}\n"
        f"<b>Duration:</b> {duration}s\n"
        f"<b>Run:</b> <code>{run_id}</code>\n"
    )

    if log_url:
        msg += f"<a href='{log_url}'>🔍 View Airflow Logs</a>\n"

    if exception:
        msg += f"\n<b>Error:</b> <code>{str(exception)[:500]}</code>"

    return msg


def send_telegram_message(context: dict[str, Any], status: str = "failure"):
    token, chat_id = _get_telegram_creds()
    if not token or not chat_id:
        return

    emoji = "❌" if status == "failure" else "✅"
    message = build_message(context, status, emoji)

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        response = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        response.raise_for_status()
    except Exception as e:
        log.warning(f"Telegram notification failed: {e}")


def on_failure_callback(context):
    send_telegram_message(context, status="failure")


def on_success_callback(context):
    send_telegram_message(context, status="success")
