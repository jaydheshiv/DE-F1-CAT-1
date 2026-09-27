"""
Pipeline Alerting DAG — Phase II
Monitors pipeline health and sends alerts for anomalies.
Runs every 30 minutes.
"""
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import sys as _sys
from pathlib import Path as _Path
_monitoring_paths = [
    str(_Path(__file__).resolve().parents[2] / "monitoring"),
    "/opt/airflow/monitoring",
]
for _p in _monitoring_paths:
    if _p not in _sys.path:
        _sys.path.insert(0, _p)

try:
    from alerting import (
        send_email_alert, send_slack_alert,
        alert_on_failure, alert_on_success,
    )
except ImportError:
    from functools import partial
    send_email_alert = lambda *a, **kw: None
    send_slack_alert = lambda *a, **kw: None
    alert_on_failure = None
    alert_on_success = None

logger = logging.getLogger(__name__)


def check_streaming_health(**context):
    """
    Check if the Spark streaming pipeline is healthy:
    - Are records being written to Iceberg recently?
    - Is the DLQ growing abnormally?
    """
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "labdb"),
        user=os.environ.get("POSTGRES_USER", "labadmin"),
        password=os.environ.get("POSTGRES_PASSWORD", "labpassword"),
    )
    cur = conn.cursor()

    # Check recent streaming events (last hour)
    cur.execute("""
        SELECT count(*) FROM core.streaming_lap_events
        WHERE ingested_at > NOW() - INTERVAL '1 hour'
    """)
    recent_count = cur.fetchone()[0]

    # Check DLQ growth
    cur.execute("""
        SELECT count(*) FROM audit.pipeline_errors
        WHERE failed_at > NOW() - INTERVAL '1 hour'
    """)
    dlq_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    logger.info(
        f"Streaming health: {recent_count} events in last hour, "
        f"{dlq_count} DLQ entries"
    )

    context["ti"].xcom_push(key="recent_events", value=recent_count)
    context["ti"].xcom_push(key="recent_dlq", value=dlq_count)

    # Alert if DLQ has too many entries
    if dlq_count > 50:
        send_slack_alert(
            f"⚠️ *High DLQ Volume*: {dlq_count} errors in the last hour.\n"
            f"Recent valid events: {recent_count}",
            severity="warning"
        )
        send_email_alert(
            "High Dead Letter Queue Volume",
            f"DLQ entries in last hour: {dlq_count}\n"
            f"Valid events in last hour: {recent_count}\n"
            f"Please investigate pipeline errors.",
            severity="warning"
        )


def check_pipeline_latency(**context):
    """
    Monitor pipeline latency by checking audit.pipeline_metrics.
    Alerts if average latency exceeds threshold.
    """
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "labdb"),
        user=os.environ.get("POSTGRES_USER", "labadmin"),
        password=os.environ.get("POSTGRES_PASSWORD", "labpassword"),
    )
    cur = conn.cursor()

    cur.execute("""
        SELECT AVG(metric_value), MAX(metric_value)
        FROM audit.pipeline_metrics
        WHERE metric_name = 'latency_ms'
          AND recorded_at > NOW() - INTERVAL '30 minutes'
    """)
    row = cur.fetchone()
    avg_latency = row[0]
    max_latency = row[1]

    cur.close()
    conn.close()

    if avg_latency is not None:
        logger.info(f"Pipeline latency: avg={avg_latency:.1f}ms, max={max_latency:.1f}ms")

        if avg_latency > 5000:  # > 5 seconds average
            send_slack_alert(
                f"🐌 *High Pipeline Latency*\n"
                f"Average: {avg_latency:.1f}ms | Max: {max_latency:.1f}ms\n"
                f"Threshold: 5000ms",
                severity="warning"
            )
    else:
        logger.info("No latency metrics recorded in the last 30 minutes")


def check_airflow_task_failures(**context):
    """
    Check for any Airflow task failures in the last hour.
    """
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.state import State
    from airflow.utils.session import provide_session

    @provide_session
    def _count_failures(session=None):
        cutoff = datetime.utcnow() - timedelta(hours=1)
        failed = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.state == State.FAILED,
                TaskInstance.end_date >= cutoff,
            )
            .count()
        )
        return failed

    failures = _count_failures()
    logger.info(f"Airflow task failures in last hour: {failures}")

    if failures > 0:
        send_slack_alert(
            f"🔴 *{failures} Airflow task failure(s)* in the last hour.\n"
            f"Check Airflow UI for details.",
            severity="critical"
        )

    context["ti"].xcom_push(key="task_failures", value=failures)


# ── DAG Definition ───────────────────────────────────────────
default_args = {
    "owner": "f1-data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

if alert_on_failure:
    default_args["on_failure_callback"] = alert_on_failure

with DAG(
    dag_id="pipeline_alerting",
    default_args=default_args,
    description="Monitors pipeline health, latency, and DLQ volume — sends email/Slack alerts",
    schedule_interval="*/30 * * * *",  # Every 30 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["phase2", "monitoring", "alerting"],
) as dag:

    start = EmptyOperator(task_id="start")

    t_streaming = PythonOperator(
        task_id="check_streaming_health",
        python_callable=check_streaming_health,
    )

    t_latency = PythonOperator(
        task_id="check_pipeline_latency",
        python_callable=check_pipeline_latency,
    )

    t_failures = PythonOperator(
        task_id="check_airflow_task_failures",
        python_callable=check_airflow_task_failures,
    )

    end = EmptyOperator(task_id="end")

    start >> [t_streaming, t_latency, t_failures] >> end
