# Monitoring package — Phase II
from monitoring.logging_config import setup_logging, get_logger, new_correlation_id
from monitoring.alerting import send_email_alert, send_slack_alert, alert_on_failure
from monitoring.metrics import collector as metrics_collector
