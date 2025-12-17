import requests
from airflow.hooks.base import BaseHook
from utils.dbt_logger import log


def get_telegram_creds():
    try:
        conn = BaseHook.get_connection("telegram_default")
        return conn.password, conn.host
    except Exception:
        log.error("Failed to retrieve Telegram connection 'telegram_default'.")
        return None, None


def send_telegram_message(context, status="failure"):
    token, chat_id = get_telegram_creds()
    if not token:
        return

    dag_id = context.get("task_instance").dag_id
    task_id = context.get("task_instance").task_id

    emoji = "🔴" if status == "failure" else "🟢"
    message = f"{emoji} DAG: {dag_id}\nTask: {task_id}\nStatus: {status.upper()}"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": message}, timeout=10)
        log.info(f"Telegram alert sent for {dag_id}")
    except Exception as e:
        log.error(f"Telegram send failed: {e}")


def on_failure_callback(context):
    send_telegram_message(context, status="failure")


def on_success_callback(context):
    send_telegram_message(context, status="success")
