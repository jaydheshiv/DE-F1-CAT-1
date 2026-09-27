# Current Architecture: F1 Data Engineering Platform

## 1. Project Folders
- `airflow/`: Contains Apache Airflow configuration and `dags/` (e.g., `f1_etl_pipeline.py`, `f1_historical_pipeline.py`).
- `api/`: Contains the FastAPI application connecting to PostgreSQL.
- `dashboard/`: Contains the Streamlit analytical dashboard (`app.py`).
- `frontend/`: Contains the React dashboard application.
- `sql/`: Contains PostgreSQL initialization scripts (`init.sql`).
- `streaming/`: Contains Kafka `producer.py` and `consumer.py` simulating real-time laps.
- `f1 db/`: Contains raw CSV data files for historical loading and simulation.

## 2. Docker Services
- `postgres`: PostgreSQL database (port 5432)
- `zookeeper`: Confluent Zookeeper for Kafka (port 2181)
- `kafka`: Confluent Kafka broker (port 9092)
- `airflow-init`: Initialization container for Airflow
- `airflow-webserver`: Airflow UI (port 8080)
- `airflow-scheduler`: Airflow DAG scheduler
- `api`: FastAPI backend (port 8000)
- `producer`: Python Kafka producer pushing lap events
- `consumer`: Python Kafka consumer reading lap events to Postgres
- `dashboard`: Streamlit analytics dashboard (port 8501)
- `frontend`: React frontend dashboard (port 5173)

## 3. Database Schema (PostgreSQL `labdb`)
### Fact Tables
- `fact_race_results`
- `fact_driver_standings`
- `fact_constructor_standings`

### Dimension Tables
- `dim_driver`
- `dim_circuit`
- `dim_constructor`
- `dim_race`

### Streaming & Error Tables
- `streaming_lap_events`
- `pipeline_errors` (Dead Letter Queue)

## 4. Airflow DAGs
- `f1_etl_pipeline.py`: Main ETL DAG extracting from CSVs, cleaning, and idempotently upserting into the Star Schema.
- `f1_historical_pipeline.py`: Legacy/deprecated pipeline.

## 5. Streaming Architecture (Kafka)
- **Topic:** `f1-lap-events`
- **Producer:** Reads `lap_times.csv` from `f1 db/`, finds a race, and emits 1 event per second. Injects invalid events (negative times, missing drivers) intentionally.
- **Consumer:** Consumes `f1-lap-events`, validates data. Valid events go to `streaming_lap_events`, invalid to `pipeline_errors`.

## 6. Serving Layer
- **API (FastAPI):** Exposes F1 data via dynamic endpoints, pagination, and OLAP query endpoints.
  - **Phase II:** `/api/v2/lakehouse/*` endpoints powered by Trino queries against Iceberg.
- **React Frontend:** Displays Hero stats, tables, CDC live streaming, and **Lakehouse** tab (Phase II).
- **Streamlit Dashboard:** Displays interactive charts (Driver/Constructor standings, Profiles, Maps, DLQ monitor).

## 7. Phase II: Lakehouse Architecture
- **MinIO (port 9000/9001):** S3-compatible object storage for Iceberg data files.
- **Hive Metastore (port 9083):** Apache Iceberg catalog backend.
- **PySpark Structured Streaming (port 4040):** Kafka → Iceberg streaming pipeline.
- **Trino (port 8090):** Distributed SQL engine querying Iceberg tables.

### Iceberg Tables
- `lakehouse.raw_lap_events` — All Kafka events, partitioned by `race_id`
- `lakehouse.dead_letter_queue` — Invalid events with error metadata
- `lakehouse.cleaned_laps` — Validated, enriched lap data
- `lakehouse.agg_driver_race_stats` — Aggregated per-driver statistics

### Phase II Airflow DAGs
- `iceberg_maintenance` — Compaction, snapshot expiry, orphan cleanup (every 6h)
- `pipeline_alerting` — Health monitoring, latency checks, DLQ alerts (every 30m)
- `data_lineage` — Lineage record seeding and report generation (daily)

## 8. Observability (Phase II)
- **Structured Logging:** JSON-formatted logs with correlation IDs (`monitoring/logging_config.py`)
- **Alerting:** Email (SMTP) + Slack (webhook) for pipeline failures (`monitoring/alerting.py`)
- **Metrics:** Prometheus counters, histograms, gauges at `/metrics` (`monitoring/metrics.py`)
- **Data Lineage:** Source-to-destination tracking with column mappings (`monitoring/lineage.py`)

