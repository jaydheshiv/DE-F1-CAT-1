-- Migration script: Move public tables to advanced schemas and set search_path
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS audit;

-- Move existing star schema to warehouse
ALTER TABLE IF EXISTS public.dim_driver SET SCHEMA warehouse;
ALTER TABLE IF EXISTS public.dim_circuit SET SCHEMA warehouse;
ALTER TABLE IF EXISTS public.dim_constructor SET SCHEMA warehouse;
ALTER TABLE IF EXISTS public.dim_race SET SCHEMA warehouse;
ALTER TABLE IF EXISTS public.fact_race_results SET SCHEMA warehouse;
ALTER TABLE IF EXISTS public.fact_driver_standings SET SCHEMA warehouse;
ALTER TABLE IF EXISTS public.fact_constructor_standings SET SCHEMA warehouse;

-- Move streaming and error tables
ALTER TABLE IF EXISTS public.streaming_lap_events SET SCHEMA core;
ALTER TABLE IF EXISTS public.pipeline_errors SET SCHEMA audit;
ALTER TABLE IF EXISTS public.etl_run_log SET SCHEMA audit;
ALTER TABLE IF EXISTS public.etl_staging SET SCHEMA staging;
ALTER TABLE IF EXISTS public.idempotency_test SET SCHEMA audit;

-- Recreate views in warehouse schema
DROP VIEW IF EXISTS public.v_driver_championship;
DROP VIEW IF EXISTS public.v_constructor_championship;

CREATE OR REPLACE VIEW warehouse.v_driver_championship AS
SELECT
    dd.driver_name, dd.nationality, dd.code, dr.year,
    SUM(fr.points) AS total_points,
    SUM(CASE WHEN fr.finish_position = 1 THEN 1 ELSE 0 END) AS wins,
    COUNT(fr.result_id) AS races_entered
FROM warehouse.fact_race_results fr
JOIN warehouse.dim_driver dd ON fr.driver_id = dd.driver_id
JOIN warehouse.dim_race dr ON fr.race_id = dr.race_id
GROUP BY dd.driver_name, dd.nationality, dd.code, dr.year;

CREATE OR REPLACE VIEW warehouse.v_constructor_championship AS
SELECT
    dc.name AS constructor_name, dc.nationality, dr.year,
    SUM(fr.points) AS total_points,
    SUM(CASE WHEN fr.finish_position = 1 THEN 1 ELSE 0 END) AS wins,
    COUNT(fr.result_id) AS races
FROM warehouse.fact_race_results fr
JOIN warehouse.dim_constructor dc ON fr.constructor_id = dc.constructor_id
JOIN warehouse.dim_race dr ON fr.race_id = dr.race_id
GROUP BY dc.name, dc.nationality, dr.year;

-- Create New Audit Tables
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100),
    status VARCHAR(20),
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    records_extracted INT DEFAULT 0,
    records_loaded INT DEFAULT 0,
    records_failed INT DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS audit.data_quality_results (
    id SERIAL PRIMARY KEY,
    run_id INT,
    source_name VARCHAR(100),
    table_name VARCHAR(100),
    check_name VARCHAR(100),
    records_checked INT,
    records_failed INT,
    status VARCHAR(20),
    error_details TEXT,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit.pipeline_watermarks (
    source_name VARCHAR(100),
    entity_name VARCHAR(100),
    last_watermark TIMESTAMP,
    last_successful_run TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_name, entity_name)
);

CREATE TABLE IF NOT EXISTS audit.processed_events (
    event_id VARCHAR(100) PRIMARY KEY,
    topic VARCHAR(100),
    partition INT,
    offset_num BIGINT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit.change_log (
    change_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    record_id VARCHAR(100),
    operation VARCHAR(10),
    old_value JSONB,
    new_value JSONB,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create Staging Images Table
CREATE TABLE IF NOT EXISTS staging.images (
    entity_type VARCHAR(50),
    entity_id VARCHAR(100),
    entity_name VARCHAR(150),
    image_url TEXT,
    image_source VARCHAR(100),
    license VARCHAR(100),
    attribution TEXT,
    retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_type, entity_id)
);

-- Educational Snowflake Views
CREATE OR REPLACE VIEW warehouse.dim_nationality AS
SELECT DISTINCT nationality FROM warehouse.dim_driver WHERE nationality IS NOT NULL;

CREATE OR REPLACE VIEW warehouse.dim_country AS
SELECT DISTINCT country FROM warehouse.dim_circuit WHERE country IS NOT NULL;

-- Set search_path so existing Python code doesn't break
ALTER ROLE labadmin SET search_path TO warehouse, core, audit, staging, public;
