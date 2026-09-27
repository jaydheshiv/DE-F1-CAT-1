# Phase II Architecture: Production-Ready Real-Time Lakehouse

## 1. Overview

Phase II upgrades the F1 Data Engineering platform from a batch-to-Postgres pipeline
into an enterprise-grade **lakehouse architecture** with real-time streaming, open
table formats (Apache Iceberg), distributed SQL (Trino), and full observability.

## 2. Architecture Diagram

```
┌─────────────┐    ┌──────────┐    ┌──────────────┐    ┌────────────────────┐
│  F1 CSV DB  │───▶│ Producer │───▶│ Kafka Topic  │───▶│ PySpark Structured │
│  (Raw Data) │    │(Streaming│    │f1-lap-events │    │    Streaming       │
└─────────────┘    └──────────┘    └──────────────┘    └─────────┬──────────┘
                                                                 │
                                                    ┌────────────▼────────────┐
                                                    │  MinIO (S3-compatible)  │
                                                    │  Apache Iceberg Tables  │
                                                    │  ├── raw_lap_events     │
                                                    │  ├── cleaned_laps       │
                                                    │  ├── agg_driver_stats   │
                                                    │  └── dead_letter_queue  │
                                                    └────────────┬────────────┘
                                                                 │
                    ┌──────────┐    ┌──────────────┐             │
                    │ React    │◀───│   Trino      │◀────────────┘
                    │ Dashboard│    │(Distributed  │
                    └──────────┘    │   SQL)       │    ┌──────────────┐
                                    └──────────────┘    │   Airflow    │
                                                        │ (Compaction, │
                    ┌──────────────────────────────┐    │  Partitions) │
                    │  Observability Stack          │    └──────────────┘
                    │  ├── Structured Logging       │
                    │  ├── Prometheus Metrics        │
                    │  ├── Email/Slack Alerts        │
                    │  └── Data Lineage Tracker      │
                    └──────────────────────────────┘
```

## 3. New Services (Phase II)

| Service | Tech | Port | Description |
|---------|------|------|-------------|
| `minio` | MinIO | 9000/9001 | S3-compatible object storage for Iceberg data |
| `hive-metastore` | Apache Hive 4.0 | 9083 | Iceberg catalog (Hive Metastore) |
| `spark-iceberg` | PySpark 3.5 + Iceberg | 4040 | Structured Streaming (Kafka → Iceberg) |
| `trino` | Trino 443 | 8090 | Distributed SQL engine for BI queries |

## 4. Iceberg Tables

| Table | Partitioning | Description |
|-------|-------------|-------------|
| `lakehouse.raw_lap_events` | `race_id` | All events (valid + invalid), raw from Kafka |
| `lakehouse.dead_letter_queue` | None | Invalid events with error details |
| `lakehouse.cleaned_laps` | `race_id` | Validated, enriched lap events |
| `lakehouse.agg_driver_race_stats` | `race_id` | Aggregated per-driver per-race statistics |

## 5. Data Flow

### Ingestion Path
1. **F1 CSV → Kafka Producer** — reads `lap_times.csv`, emits JSON events
2. **Kafka Topic `f1-lap-events`** — message broker
3. **PySpark Structured Streaming** — reads from Kafka, processes in micro-batches
4. **Iceberg Tables (MinIO)** — stores data in Parquet format with Iceberg metadata

### Serving Path
1. **Trino** — queries Iceberg tables via Hive Metastore catalog
2. **FastAPI** — `/api/v2/lakehouse/*` endpoints backed by Trino
3. **React Dashboard** — "Lakehouse" tab with real-time data

## 6. Observability Stack

### Structured Logging
- JSON-formatted logs with correlation IDs
- Component tags for filtering (producer, spark, api, airflow)
- Centralized via `monitoring/logging_config.py`

### Alerting
- **Email** — SMTP-based alerts for pipeline failures
- **Slack** — Webhook-based alerts with severity levels
- **Airflow Callbacks** — `on_failure_callback` on all DAGs
- Configured via environment variables

### Metrics
- Prometheus-compatible counters, histograms, gauges
- Pipeline latency, throughput, error rates
- Kafka consumer lag monitoring
- Exposed at `/metrics` endpoint

## 7. Data Lineage

Tracks data from origin (CSV/Kafka) through transformations to destination (Iceberg/API):
- Source system → Transform → Destination
- Column-level mappings
- Stored in `audit.data_lineage` table
- Visualized as Mermaid diagrams in the dashboard

## 8. Table Maintenance (Airflow)

| DAG | Schedule | Tasks |
|-----|----------|-------|
| `iceberg_maintenance` | Every 6h | Compaction, snapshot expiry, orphan cleanup, stats |
| `pipeline_alerting` | Every 30m | Health checks, latency monitoring, DLQ alerts |
| `data_lineage` | Daily | Seed lineage records, generate reports |

## 9. Deployment

### Local (Docker Compose)
```bash
# Start Phase I + Phase II services
docker compose -f docker-compose.yml -f docker-compose.phase2.yml up -d --build

# Access UIs
# MinIO Console:  http://localhost:9001  (minioadmin/minioadmin)
# Spark UI:       http://localhost:4040
# Trino UI:       http://localhost:8090
# React Dashboard: http://localhost:5173 → Lakehouse tab
# FastAPI Docs:   http://localhost:8000/docs
# Airflow:        http://localhost:8080  (admin/admin)
```

### Cloud Deployment
Configure environment variables for cloud-native services:
- Replace MinIO with AWS S3 / GCS / Azure Blob
- Replace Hive Metastore with AWS Glue / Nessie
- Replace local Spark with EMR / Dataproc
- Replace local Trino with Athena / BigQuery

## 10. Directory Structure (Phase II Additions)

```
DE-F1-CAT-1/
├── lakehouse/
│   ├── spark/
│   │   ├── streaming_to_iceberg.py     # PySpark Structured Streaming
│   │   ├── Dockerfile.spark            # Custom Spark + Iceberg image
│   │   └── spark-defaults.conf         # Spark configuration
│   ├── trino/
│   │   └── etc/
│   │       ├── config.properties       # Trino server config
│   │       ├── jvm.config              # JVM settings
│   │       ├── node.properties         # Node identity
│   │       └── catalog/
│   │           ├── iceberg.properties  # Iceberg connector
│   │           └── postgresql.properties # Postgres connector
│   └── minio-init.sh                   # Bucket bootstrap
├── monitoring/
│   ├── __init__.py
│   ├── logging_config.py               # Structured JSON logging
│   ├── alerting.py                     # Email + Slack alerts
│   ├── metrics.py                      # Prometheus metrics
│   ├── lineage.py                      # Data lineage tracker
│   └── prometheus.yml                  # Prometheus config
├── airflow/dags/
│   ├── iceberg_maintenance.py          # Table maintenance DAG
│   ├── pipeline_alerting.py            # Health monitoring DAG
│   └── lineage_dag.py                  # Lineage seeding DAG
├── sql/
│   └── phase2_schema.sql               # Phase II DB tables
├── docker-compose.phase2.yml           # Phase II services overlay
└── docs/
    └── phase2_architecture.md          # This document
```
