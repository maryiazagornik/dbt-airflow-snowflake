import logging
import requests
from airflow.hooks.base import BaseHook
import os


logger = logging.getLogger(__name__)


def get_telegram_creds():
    token = None
    chat_id = None

    try:
        conn = BaseHook.get_connection("telegram_default")
        token = conn.password
        chat_id = conn.host
        logger.info("Retrieved Telegram credentials from Airflow Connection.")
    except Exception:
        logger.debug(
            "No Airflow connection found for 'telegram_default'. trying env vars."
        )

    if not token:
        token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not chat_id:
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

    return token, chat_id


def send_telegram_message(context, status):
    token, chat_id = get_telegram_creds()

    if not token or not chat_id:
        logger.warning("Telegram credentials not found. Skipping alert.")
        return

    # Данные из контекста Airflow
    dag_id = context.get("task_instance").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    exception = context.get("exception")

    if status == "failure":
        emoji = "🔴"
        title = "Task Failed"
        error_msg = f"\n**Error:** `{str(exception)[:200]}...`" if exception else ""
    else:
        emoji = "🟢"
        title = "Pipeline Succeeded"
        error_msg = ""
        task_id = "All Tasks"

    message = (
        f"{emoji} **{title}**\n\n"
        f"**DAG:** `{dag_id}`\n"
        f"**Task:** `{task_id}`\n"
        f"**Time:** `{execution_date}`"
        f"{error_msg}\n\n"
        f"[View Logs]({log_url})"
    )

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info(f"Telegram notification sent: {status}")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")


def on_failure_callback(context):
    send_telegram_message(context, status="failure")


def on_success_callback(context):
    send_telegram_message(context, status="success")
