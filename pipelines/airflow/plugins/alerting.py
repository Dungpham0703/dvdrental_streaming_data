import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)


def send_pipeline_alert(context: dict[str, Any]) -> None:
    ti = context.get("task_instance")
    dag_id = ti.dag_id if ti else "Unknown_DAG"
    task_id = ti.task_id if ti else "Unknown_Task"
    execution_date = context.get("logical_date") or context.get("execution_date")
    log_url = ti.log_url if ti else "N/A"
    exception = context.get("exception")

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")

    error_summary = (
        f"*[DATA PIPELINE FAILURE]*\n"
        f"• *DAG:* `{dag_id}`\n"
        f"• *Task:* `{task_id}`\n"
        f"• *Execution Time:* `{execution_date}`\n"
        f"• *Error:* `{exception}`\n"
        f"• *Log URL:* <{log_url}|View Airflow Task Log>\n"
        f"• *Action Required:* Inspect Athena `audit` schema tables for stored dbt failure rows."
    )

    if not webhook_url:
        logger.warning(
            "[ALERT NOTIFICATION] No SLACK_WEBHOOK_URL set. "
            "Pipeline error details:\n%s",
            error_summary,
        )
        return

    payload = {
        "text": error_summary,
        "attachments": [
            {
                "color": "#D32F2F",
                "fields": [
                    {"title": "DAG", "value": dag_id, "short": True},
                    {"title": "Task", "value": task_id, "short": True},
                    {
                        "title": "Execution Time",
                        "value": str(execution_date),
                        "short": False,
                    },
                ],
            }
        ],
    }

    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            logger.info(
                "[ALERT NOTIFICATION] Alert successfully sent to webhook. Status: %s",
                response.status,
            )
    except urllib.error.URLError as err:
        logger.error("[ALERT NOTIFICATION] Failed to send webhook alert: %s", err)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if not os.environ.get("SLACK_WEBHOOK_URL"):
        env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    if line.startswith("SLACK_WEBHOOK_URL="):
                        os.environ["SLACK_WEBHOOK_URL"] = line.strip().split("=", 1)[1]
