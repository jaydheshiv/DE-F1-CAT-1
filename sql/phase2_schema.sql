-- ============================================================
-- PHASE II: Production-Ready Lakehouse — Additional Schema
-- Data Lineage, Pipeline Metrics, Alert Logging
-- ============================================================

-- ── Data Lineage ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit.data_lineage (
    lineage_id   SERIAL PRIMARY KEY,
    source_system   VARCHAR(100) NOT NULL,   -- e.g. 'kafka:f1-lap-events', 'csv:drivers.csv'
    source_table    VARCHAR(150),            -- e.g. 'f1-lap-events' or NULL for raw files
    transform_name  VARCHAR(150) NOT NULL,   -- e.g. 'spark_streaming_to_iceberg'
    dest_system     VARCHAR(100) NOT NULL,   -- e.g. 'iceberg:lakehouse'
    dest_table      VARCHAR(150) NOT NULL,   -- e.g. 'raw_lap_events'
    column_mappings JSONB,                   -- {"source_col": "dest_col", ...}
    description     TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Pipeline Metrics ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit.pipeline_metrics (
    metric_id       SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(100) NOT NULL,
    metric_name     VARCHAR(100) NOT NULL,   -- e.g. 'records_per_second', 'latency_ms'
    metric_value    DECIMAL(12,4),
    labels          JSONB,                   -- {"topic": "f1-lap-events", "partition": "0"}
    recorded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_name
    ON audit.pipeline_metrics (pipeline_name, metric_name, recorded_at);

-- ── Alert Log ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit.alert_log (
    alert_id        SERIAL PRIMARY KEY,
    alert_type      VARCHAR(50) NOT NULL,    -- 'email', 'slack'
    severity        VARCHAR(20) NOT NULL,    -- 'info', 'warning', 'critical'
    subject         VARCHAR(255),
    message         TEXT,
    recipient       VARCHAR(255),
    status          VARCHAR(20) DEFAULT 'sent',  -- 'sent', 'failed'
    error_detail    TEXT,
    sent_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ── Iceberg Table Registry (for tracking table metadata in Postgres) ──
CREATE TABLE IF NOT EXISTS audit.iceberg_table_registry (
    table_id        SERIAL PRIMARY KEY,
    catalog_name    VARCHAR(100) NOT NULL,
    schema_name     VARCHAR(100) NOT NULL,
    table_name      VARCHAR(100) NOT NULL,
    location        TEXT,
    partition_spec  JSONB,
    last_compaction TIMESTAMP,
    last_snapshot_expiry TIMESTAMP,
    snapshot_count  INT DEFAULT 0,
    file_count      INT DEFAULT 0,
    total_records   BIGINT DEFAULT 0,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (catalog_name, schema_name, table_name)
);
