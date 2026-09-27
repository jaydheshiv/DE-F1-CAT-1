"""
Data Lineage Tracker — Phase II Governance
Records and visualizes the flow of data from origin to destination.
Supports column-level lineage and generates Mermaid diagrams.
"""
import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

logger = logging.getLogger(__name__)

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "labdb")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "labadmin")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "labpassword")


def _get_conn():
    if not HAS_PSYCOPG2:
        raise RuntimeError("psycopg2 not available")
    return psycopg2.connect(
        host=POSTGRES_HOST, database=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )


# ══════════════════════════════════════════════════════════════
# Lineage Registration
# ══════════════════════════════════════════════════════════════

def register_lineage(
    source_system: str,
    source_table: Optional[str],
    transform_name: str,
    dest_system: str,
    dest_table: str,
    column_mappings: Optional[dict] = None,
    description: Optional[str] = None,
):
    """
    Register a lineage record (source → transform → destination).

    Example:
        register_lineage(
            source_system="kafka:f1-lap-events",
            source_table="f1-lap-events",
            transform_name="spark_structured_streaming",
            dest_system="iceberg:lakehouse",
            dest_table="raw_lap_events",
            column_mappings={
                "race_id": "race_id",
                "driver_id": "driver_id",
                "lap": "lap",
                "position": "position",
                "lap_time": "lap_time",
                "milliseconds": "milliseconds",
            },
            description="Raw Kafka events ingested into Iceberg via Spark Structured Streaming"
        )
    """
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO audit.data_lineage
                (source_system, source_table, transform_name,
                 dest_system, dest_table, column_mappings, description, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT DO NOTHING
        """, (
            source_system, source_table, transform_name,
            dest_system, dest_table,
            json.dumps(column_mappings) if column_mappings else None,
            description,
        ))
        conn.commit()
        cur.close()
        conn.close()
        logger.info(
            f"Lineage registered: {source_system}/{source_table} "
            f"--[{transform_name}]--> {dest_system}/{dest_table}"
        )
    except Exception as e:
        logger.error(f"Failed to register lineage: {e}")


def get_all_lineage() -> list[dict]:
    """Retrieve all lineage records from the database."""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT lineage_id, source_system, source_table, transform_name,
                   dest_system, dest_table, column_mappings, description,
                   created_at, updated_at
            FROM audit.data_lineage
            ORDER BY created_at
        """)
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        logger.error(f"Failed to fetch lineage: {e}")
        return []


def generate_mermaid_diagram() -> str:
    """
    Generate a Mermaid flowchart diagram from lineage records.
    Returns a Mermaid-syntax string.
    """
    records = get_all_lineage()
    if not records:
        return "graph LR\n  NO_DATA[No lineage records found]"

    lines = ["graph LR"]
    nodes = set()

    for rec in records:
        src = rec["source_system"].replace(":", "_").replace("-", "_")
        dst = rec["dest_system"].replace(":", "_").replace("-", "_")
        src_table = (rec["source_table"] or "source").replace("-", "_")
        dst_table = rec["dest_table"].replace("-", "_")
        transform = rec["transform_name"].replace("-", "_").replace(" ", "_")

        src_node = f"{src}__{src_table}"
        dst_node = f"{dst}__{dst_table}"

        # Add node labels if new
        if src_node not in nodes:
            src_label = f'{rec["source_system"]}/{rec["source_table"] or "∗"}'
            lines.append(f'  {src_node}["{src_label}"]')
            nodes.add(src_node)

        if dst_node not in nodes:
            dst_label = f'{rec["dest_system"]}/{rec["dest_table"]}'
            lines.append(f'  {dst_node}["{dst_label}"]')
            nodes.add(dst_node)

        # Add edge with transform label
        lines.append(f"  {src_node} -->|{transform}| {dst_node}")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════
# Pre-defined Lineage for F1 Pipeline
# ══════════════════════════════════════════════════════════════

F1_LINEAGE_DEFINITIONS = [
    # Phase I: CSV → Postgres
    {
        "source_system": "csv:f1db",
        "source_table": "drivers.csv",
        "transform_name": "airflow_etl_pipeline",
        "dest_system": "postgres:labdb",
        "dest_table": "warehouse.dim_driver",
        "column_mappings": {
            "driverRef": "driver_id",
            "forename+surname": "driver_name",
            "code": "code",
            "number": "number",
            "nationality": "nationality",
            "dob": "dob",
            "url": "wiki_url",
        },
        "description": "Driver dimension loaded from CSV via Airflow ETL"
    },
    {
        "source_system": "csv:f1db",
        "source_table": "constructors.csv",
        "transform_name": "airflow_etl_pipeline",
        "dest_system": "postgres:labdb",
        "dest_table": "warehouse.dim_constructor",
        "column_mappings": {
            "constructorRef": "constructor_id",
            "name": "name",
            "nationality": "nationality",
            "url": "wiki_url",
        },
        "description": "Constructor dimension loaded from CSV via Airflow ETL"
    },
    {
        "source_system": "csv:f1db",
        "source_table": "circuits.csv",
        "transform_name": "airflow_etl_pipeline",
        "dest_system": "postgres:labdb",
        "dest_table": "warehouse.dim_circuit",
        "description": "Circuit dimension loaded from CSV via Airflow ETL"
    },
    {
        "source_system": "csv:f1db",
        "source_table": "races.csv",
        "transform_name": "airflow_etl_pipeline",
        "dest_system": "postgres:labdb",
        "dest_table": "warehouse.dim_race",
        "description": "Race dimension loaded from CSV via Airflow ETL"
    },
    {
        "source_system": "csv:f1db",
        "source_table": "results.csv",
        "transform_name": "airflow_etl_pipeline",
        "dest_system": "postgres:labdb",
        "dest_table": "warehouse.fact_race_results",
        "description": "Race results fact table loaded from CSV via Airflow ETL"
    },
    # Phase I: Kafka → Postgres
    {
        "source_system": "csv:f1db",
        "source_table": "lap_times.csv",
        "transform_name": "kafka_producer",
        "dest_system": "kafka",
        "dest_table": "f1-lap-events",
        "column_mappings": {
            "raceId": "race_id",
            "driverId": "driver_id",
            "lap": "lap",
            "position": "position",
            "time": "lap_time",
            "milliseconds": "milliseconds",
        },
        "description": "Lap times streamed to Kafka topic by producer"
    },
    {
        "source_system": "kafka",
        "source_table": "f1-lap-events",
        "transform_name": "kafka_consumer_postgres",
        "dest_system": "postgres:labdb",
        "dest_table": "core.streaming_lap_events",
        "description": "Valid Kafka events consumed and written to Postgres"
    },
    # Phase II: Kafka → Spark → Iceberg
    {
        "source_system": "kafka",
        "source_table": "f1-lap-events",
        "transform_name": "spark_structured_streaming",
        "dest_system": "iceberg:lakehouse",
        "dest_table": "raw_lap_events",
        "column_mappings": {
            "race_id": "race_id",
            "driver_id": "driver_id",
            "driver_code": "driver_code",
            "driver_name": "driver_name",
            "lap": "lap",
            "position": "position",
            "lap_time": "lap_time",
            "milliseconds": "milliseconds",
        },
        "description": "All Kafka events (valid + invalid) written to raw Iceberg table via Spark"
    },
    {
        "source_system": "kafka",
        "source_table": "f1-lap-events",
        "transform_name": "spark_validation",
        "dest_system": "iceberg:lakehouse",
        "dest_table": "dead_letter_queue",
        "description": "Invalid events routed to Iceberg DLQ by Spark"
    },
    {
        "source_system": "iceberg:lakehouse",
        "source_table": "raw_lap_events",
        "transform_name": "spark_enrichment",
        "dest_system": "iceberg:lakehouse",
        "dest_table": "cleaned_laps",
        "column_mappings": {
            "milliseconds": "seconds (computed: ms/1000)",
        },
        "description": "Valid events enriched with derived fields and written to cleaned table"
    },
    {
        "source_system": "iceberg:lakehouse",
        "source_table": "cleaned_laps",
        "transform_name": "spark_aggregation",
        "dest_system": "iceberg:lakehouse",
        "dest_table": "agg_driver_race_stats",
        "description": "Aggregated driver statistics computed per race from cleaned laps"
    },
    # Phase II: Iceberg → Trino → Dashboard
    {
        "source_system": "iceberg:lakehouse",
        "source_table": "cleaned_laps",
        "transform_name": "trino_query",
        "dest_system": "api:fastapi",
        "dest_table": "/api/v2/lakehouse/laps",
        "description": "Trino queries Iceberg tables and serves results via FastAPI"
    },
    {
        "source_system": "iceberg:lakehouse",
        "source_table": "agg_driver_race_stats",
        "transform_name": "trino_query",
        "dest_system": "api:fastapi",
        "dest_table": "/api/v2/lakehouse/stats",
        "description": "Aggregated stats served to React dashboard via Trino"
    },
]


def seed_lineage():
    """Seed all predefined lineage records into the database."""
    logger.info(f"Seeding {len(F1_LINEAGE_DEFINITIONS)} lineage records...")
    for definition in F1_LINEAGE_DEFINITIONS:
        register_lineage(**definition)
    logger.info("Lineage seeding complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    seed_lineage()
    print("\n=== Mermaid Lineage Diagram ===\n")
    print(generate_mermaid_diagram())
