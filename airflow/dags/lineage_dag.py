"""
Data Lineage DAG — Phase II
Scans and seeds lineage records, generates lineage reports.
Runs daily.
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
    from alerting import alert_on_failure
except ImportError:
    alert_on_failure = None

logger = logging.getLogger(__name__)


def seed_lineage_records(**context):
    """Seed all predefined lineage records into the database."""
    import sys
    sys.path.insert(0, "/opt/airflow/monitoring")
    from lineage import seed_lineage
    seed_lineage()
    logger.info("Lineage records seeded successfully")


def generate_lineage_report(**context):
    """Generate a Mermaid lineage diagram and push to XCom."""
    import sys
    sys.path.insert(0, "/opt/airflow/monitoring")
    from lineage import generate_mermaid_diagram, get_all_lineage

    diagram = generate_mermaid_diagram()
    records = get_all_lineage()

    context["ti"].xcom_push(key="lineage_diagram", value=diagram)
    context["ti"].xcom_push(key="lineage_count", value=len(records))

    logger.info(f"Generated lineage diagram with {len(records)} records")
    logger.info(f"Mermaid diagram:\n{diagram}")


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
    dag_id="data_lineage",
    default_args=default_args,
    description="Seeds data lineage records and generates lineage reports",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["phase2", "lineage", "governance"],
) as dag:

    start = EmptyOperator(task_id="start")

    t_seed = PythonOperator(
        task_id="seed_lineage_records",
        python_callable=seed_lineage_records,
    )

    t_report = PythonOperator(
        task_id="generate_lineage_report",
        python_callable=generate_lineage_report,
    )

    end = EmptyOperator(task_id="end")

    start >> t_seed >> t_report >> end
