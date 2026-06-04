"""
Slack notification utilities for Airflow DAGs
"""

import logging
import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


def send_slack_notification(message, webhook_url=None, channel=None):
    """
    Send a message to Slack
    
    Args:
        message: Message text to send
        webhook_url: Slack webhook URL (optional)
        channel: Slack channel (optional)
    
    Returns:
        True if successful, False otherwise
    """
    if webhook_url is None:
        webhook_url = Variable.get("slack_webhook_url", default="")
    
    if not webhook_url:
        logger.warning("Slack webhook URL not configured")
        return False
    
    try:
        payload = {
            "text": message,
            "username": "Airflow",
            "icon_emoji": ":robot_face:"
        }
        
        if channel:
            payload["channel"] = channel
        
        response = requests.post(webhook_url, json=payload, timeout=10)
        
        if response.status_code == 200:
            logger.info("✅ Slack notification sent successfully")
            return True
        else:
            logger.warning(f"Slack notification failed: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error sending Slack notification: {str(e)}")
        return False


def notify_dag_failure(dag_id, task_id, error_message):
    """
    Send DAG failure notification to Slack
    
    Args:
        dag_id: DAG ID
        task_id: Task ID
        error_message: Error message
    """
    message = f":x: DAG Failure\nDAG: {dag_id}\nTask: {task_id}\nError: {error_message}"
    send_slack_notification(message)


def notify_dag_success(dag_id):
    """
    Send DAG success notification to Slack
    
    Args:
        dag_id: DAG ID
    """
    message = f":white_check_mark: DAG Success\nDAG: {dag_id}"
    send_slack_notification(message)


logger.info("Slack notifier utilities loaded")
