"""
Iceberg Table Maintenance DAG — Phase II
Automates compaction, snapshot expiry, and orphan file cleanup
for all Iceberg tables in the lakehouse.
Runs every 6 hours.
"""
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ── Try to import Phase II alerting ──────────────────────────
import sys as _sys
from pathlib import Path as _Path

_this_file = _Path(__file__).resolve()
_project_root = _this_file.parents[2] if len(_this_file.parents) > 2 else _this_file.parent

for _p in [str(_project_root), str(_project_root / "monitoring"), "/opt/airflow", "/opt/airflow/monitoring"]:
    if _p not in _sys.path and (_Path(_p).exists() or _p.startswith("/opt/")):
        _sys.path.insert(0, _p)

try:
    from monitoring.alerting import alert_on_failure, alert_on_success
except ImportError:
    try:
        from alerting import alert_on_failure, alert_on_success
    except ImportError:
        alert_on_failure = None
        alert_on_success = None

logger = logging.getLogger(__name__)

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8090"))
ICEBERG_CATALOG = "iceberg"
ICEBERG_SCHEMA = "lakehouse"

ICEBERG_TABLES = [
    "raw_lap_events",
    "dead_letter_queue",
    "cleaned_laps",
    "agg_driver_race_stats",
]


def _get_trino_conn():
    """Get a Trino connection."""
    try:
        import trino
    except ImportError:
        raise RuntimeError("trino package not installed. Run: pip install trino")

    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="airflow",
        catalog=ICEBERG_CATALOG,
        schema=ICEBERG_SCHEMA,
    )


def compact_tables(**context):
    """
    Run OPTIMIZE (compaction) on all Iceberg tables.
    Rewrites small data files into larger, optimally-sized files.
    """
    conn = _get_trino_conn()
    cursor = conn.cursor()
    results = []

    for table in ICEBERG_TABLES:
        try:
            fqn = f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{table}"
            logger.info(f"Compacting table: {fqn}")

            # Iceberg OPTIMIZE via Trino rewrites small files
            cursor.execute(f"ALTER TABLE {fqn} EXECUTE optimize")
            result = cursor.fetchall()

            results.append({"table": table, "status": "compacted", "result": str(result)})
            logger.info(f"Compaction complete for {table}: {result}")
        except Exception as e:
            results.append({"table": table, "status": "error", "error": str(e)})
            logger.error(f"Compaction failed for {table}: {e}")

    cursor.close()
    conn.close()

    context["ti"].xcom_push(key="compaction_results", value=results)
    logger.info(f"Compaction run complete: {len(results)} tables processed")


def expire_snapshots(**context):
    """
    Remove old Iceberg snapshots, retaining only the last 48 hours.
    Frees storage space from old data versions.
    """
    conn = _get_trino_conn()
    cursor = conn.cursor()
    results = []

    retention_ts = (datetime.utcnow() - timedelta(hours=48)).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )

    for table in ICEBERG_TABLES:
        try:
            fqn = f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{table}"
            logger.info(f"Expiring snapshots for {fqn} older than {retention_ts}")

            cursor.execute(
                f"ALTER TABLE {fqn} EXECUTE expire_snapshots(retention_threshold => '48h')"
            )
            result = cursor.fetchall()

            results.append({"table": table, "status": "expired", "result": str(result)})
            logger.info(f"Snapshot expiry done for {table}: {result}")
        except Exception as e:
            results.append({"table": table, "status": "error", "error": str(e)})
            logger.error(f"Snapshot expiry failed for {table}: {e}")

    cursor.close()
    conn.close()

    context["ti"].xcom_push(key="snapshot_expiry_results", value=results)


def remove_orphan_files(**context):
    """
    Delete orphaned data files not referenced by any Iceberg snapshot.
    """
    conn = _get_trino_conn()
    cursor = conn.cursor()
    results = []

    for table in ICEBERG_TABLES:
        try:
            fqn = f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{table}"
            logger.info(f"Removing orphan files for {fqn}")

            cursor.execute(
                f"ALTER TABLE {fqn} EXECUTE remove_orphan_files(retention_threshold => '72h')"
            )
            result = cursor.fetchall()

            results.append({"table": table, "status": "cleaned", "result": str(result)})
            logger.info(f"Orphan file cleanup done for {table}: {result}")
        except Exception as e:
            results.append({"table": table, "status": "error", "error": str(e)})
            logger.error(f"Orphan file cleanup failed for {table}: {e}")

    cursor.close()
    conn.close()

    context["ti"].xcom_push(key="orphan_cleanup_results", value=results)


def collect_table_stats(**context):
    """
    Collect Iceberg table metadata (snapshot count, file counts, record counts)
    and persist to audit.iceberg_table_registry in Postgres.
    """
    import psycopg2

    conn_trino = _get_trino_conn()
    cursor_trino = conn_trino.cursor()

    conn_pg = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "labdb"),
        user=os.environ.get("POSTGRES_USER", "labadmin"),
        password=os.environ.get("POSTGRES_PASSWORD", "labpassword"),
    )
    cur_pg = conn_pg.cursor()

    for table in ICEBERG_TABLES:
        try:
            fqn = f"{ICEBERG_CATALOG}.{ICEBERG_SCHEMA}.{table}"

            # Get snapshot count
            cursor_trino.execute(
                f'SELECT count(*) FROM "{fqn}$snapshots"'
            )
            snapshot_count = cursor_trino.fetchone()[0]

            # Get record count
            cursor_trino.execute(f"SELECT count(*) FROM {fqn}")
            total_records = cursor_trino.fetchone()[0]

            # Upsert to Postgres registry
            cur_pg.execute("""
                INSERT INTO audit.iceberg_table_registry
                    (catalog_name, schema_name, table_name, snapshot_count, total_records, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (catalog_name, schema_name, table_name)
                DO UPDATE SET
                    snapshot_count = EXCLUDED.snapshot_count,
                    total_records = EXCLUDED.total_records,
                    updated_at = NOW()
            """, (ICEBERG_CATALOG, ICEBERG_SCHEMA, table, snapshot_count, total_records))

            logger.info(
                f"Stats for {table}: snapshots={snapshot_count}, records={total_records}"
            )
        except Exception as e:
            logger.error(f"Failed to collect stats for {table}: {e}")

    conn_pg.commit()
    cur_pg.close()
    conn_pg.close()
    cursor_trino.close()
    conn_trino.close()


# ── DAG Definition ───────────────────────────────────────────
default_args = {
    "owner": "f1-data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

if alert_on_failure:
    default_args["on_failure_callback"] = alert_on_failure

with DAG(
    dag_id="iceberg_maintenance",
    default_args=default_args,
    description="Automated Iceberg table maintenance: compaction, snapshot expiry, orphan cleanup",
    schedule="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["phase2", "iceberg", "maintenance", "lakehouse"],
) as dag:

    start = EmptyOperator(task_id="start")

    t_compact = PythonOperator(
        task_id="compact_tables",
        python_callable=compact_tables,
    )

    t_expire = PythonOperator(
        task_id="expire_snapshots",
        python_callable=expire_snapshots,
    )

    t_orphans = PythonOperator(
        task_id="remove_orphan_files",
        python_callable=remove_orphan_files,
    )

    t_stats = PythonOperator(
        task_id="collect_table_stats",
        python_callable=collect_table_stats,
    )

    end = EmptyOperator(task_id="end")

    # Compaction first, then parallel expiry + orphan cleanup, then stats
    start >> t_compact >> [t_expire, t_orphans] >> t_stats >> end
