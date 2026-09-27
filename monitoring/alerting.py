"""
Alert Dispatcher — Phase II Observability
Supports Email (SMTP) and Slack (Webhook) alerting.
Designed to be used standalone or as Airflow on_failure_callback.
"""
import os
import json
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timezone

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

logger = logging.getLogger(__name__)

# ── Configuration (via environment variables) ─────────────────
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "")

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "labdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "labadmin")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "labpassword")


def _log_alert_to_db(alert_type: str, severity: str, subject: str,
                     message: str, recipient: str, status: str,
                     error_detail: str = None):
    """Persist alert record to audit.alert_log."""
    if not HAS_PSYCOPG2:
        return
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, database=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD,
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO audit.alert_log
                (alert_type, severity, subject, message, recipient, status, error_detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (alert_type, severity, subject, message, recipient, status, error_detail))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Failed to log alert to DB: {e}")


def send_email_alert(subject: str, body: str, severity: str = "warning",
                     to_email: str = None):
    """
    Send an email alert via SMTP.

    Args:
        subject: Email subject line
        body: Email body (plain text)
        severity: Alert severity level
        to_email: Override recipient (defaults to ALERT_EMAIL_TO env var)
    """
    recipient = to_email or ALERT_EMAIL_TO
    if not SMTP_USER or not recipient:
        logger.warning("Email alert skipped: SMTP_USER or ALERT_EMAIL_TO not configured")
        _log_alert_to_db("email", severity, subject, body, recipient or "N/A",
                         "skipped", "SMTP not configured")
        return False

    try:
        msg = MIMEMultipart()
        msg["From"] = SMTP_USER
        msg["To"] = recipient
        msg["Subject"] = f"[F1-Pipeline {severity.upper()}] {subject}"

        html_body = f"""
        <html><body>
        <h2 style="color: {'#e10600' if severity == 'critical' else '#ff8800'};">
            🏎️ F1 Pipeline Alert — {severity.upper()}
        </h2>
        <p><strong>Time:</strong> {datetime.now(timezone.utc).isoformat()}</p>
        <p><strong>Subject:</strong> {subject}</p>
        <hr>
        <pre>{body}</pre>
        </body></html>
        """
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, recipient, msg.as_string())

        logger.info(f"Email alert sent to {recipient}: {subject}")
        _log_alert_to_db("email", severity, subject, body, recipient, "sent")
        return True
    except Exception as e:
        logger.error(f"Failed to send email alert: {e}")
        _log_alert_to_db("email", severity, subject, body, recipient, "failed", str(e))
        return False


def send_slack_alert(message: str, severity: str = "warning",
                     webhook_url: str = None):
    """
    Send a Slack alert via incoming webhook.

    Args:
        message: Alert message text
        severity: Alert severity level
        webhook_url: Override webhook URL (defaults to SLACK_WEBHOOK_URL env var)
    """
    url = webhook_url or SLACK_WEBHOOK_URL
    if not url:
        logger.warning("Slack alert skipped: SLACK_WEBHOOK_URL not configured")
        _log_alert_to_db("slack", severity, "Slack Alert", message,
                         "webhook", "skipped", "Webhook URL not configured")
        return False

    if not HAS_REQUESTS:
        logger.warning("Slack alert skipped: 'requests' package not installed")
        return False

    severity_emoji = {
        "info": "ℹ️", "warning": "⚠️", "critical": "🚨"
    }.get(severity, "📢")

    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{severity_emoji} F1 Pipeline — {severity.upper()}"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": message
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"⏰ {datetime.now(timezone.utc).isoformat()}"
                    }
                ]
            }
        ]
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info(f"Slack alert sent: {severity}")
        _log_alert_to_db("slack", severity, "Slack Alert", message,
                         "webhook", "sent")
        return True
    except Exception as e:
        logger.error(f"Failed to send Slack alert: {e}")
        _log_alert_to_db("slack", severity, "Slack Alert", message,
                         "webhook", "failed", str(e))
        return False


def alert_on_failure(context):
    """
    Airflow on_failure_callback compatible function.
    Sends both email and Slack alerts when a task fails.

    Usage in DAG default_args:
        default_args = {
            'on_failure_callback': alert_on_failure,
        }
    """
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("execution_date", "unknown")
    exception = context.get("exception", "No exception details")

    subject = f"Task Failed: {dag_id}.{task_id}"
    body = (
        f"DAG:            {dag_id}\n"
        f"Task:           {task_id}\n"
        f"Execution Date: {execution_date}\n"
        f"Exception:      {exception}\n"
        f"Log URL:        {task_instance.log_url if task_instance else 'N/A'}\n"
    )

    # Send both channels
    send_email_alert(subject, body, severity="critical")
    send_slack_alert(
        f"*Task Failed*: `{dag_id}.{task_id}`\n"
        f"```{exception}```\n"
        f"Execution: {execution_date}",
        severity="critical"
    )


def alert_on_success(context):
    """
    Airflow on_success_callback for critical DAGs (optional).
    """
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    send_slack_alert(
        f"✅ DAG `{dag_id}` completed successfully.",
        severity="info"
    )
