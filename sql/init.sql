-- ============================================================
-- DATA ENGINEERING LAB — Formula 1 Star Schema
-- Milestone 1: Advanced Database Architecture & Schemas
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS audit;

-- Set default search path
ALTER DATABASE labdb SET search_path TO warehouse, core, audit, staging, public;

-- ============================================================
-- AUDIT SCHEMA (Monitoring & DLQ)
-- ============================================================

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

CREATE TABLE IF NOT EXISTS audit.pipeline_errors (
    id SERIAL PRIMARY KEY,
    source_id VARCHAR(100),
    error_type VARCHAR(100),
    error_message TEXT,
    raw_payload JSONB,
    failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit.etl_run_log (
    run_id SERIAL PRIMARY KEY,
    load_type VARCHAR(30),
    rows_extracted INT,
    rows_loaded INT,
    status VARCHAR(20),
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

-- ============================================================
-- STAGING SCHEMA
-- ============================================================

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

CREATE TABLE IF NOT EXISTS staging.etl_staging (
    staging_id SERIAL PRIMARY KEY,
    source_record_id VARCHAR(80),
    raw_payload JSONB,
    validation_status VARCHAR(20) DEFAULT 'PENDING',
    validation_error TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- WAREHOUSE SCHEMA: DIMENSION TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS warehouse.dim_driver (
    driver_id VARCHAR(50) PRIMARY KEY,
    driver_name VARCHAR(100) NOT NULL,
    code VARCHAR(5),
    number INT,
    nationality VARCHAR(60),
    dob DATE,
    wiki_url TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_constructor (
    constructor_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    nationality VARCHAR(60),
    wiki_url TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_circuit (
    circuit_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    location VARCHAR(100),
    country VARCHAR(60),
    lat DECIMAL(9,6),
    lng DECIMAL(9,6),
    alt INT,
    wiki_url TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_race (
    race_id INT PRIMARY KEY,
    year INT NOT NULL,
    round INT NOT NULL,
    circuit_id VARCHAR(50) REFERENCES warehouse.dim_circuit(circuit_id),
    name VARCHAR(150) NOT NULL,
    race_date DATE,
    wiki_url TEXT
);

-- ============================================================
-- WAREHOUSE SCHEMA: FACT TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_race_results (
    result_id INT PRIMARY KEY,
    race_id INT REFERENCES warehouse.dim_race(race_id),
    driver_id VARCHAR(50) REFERENCES warehouse.dim_driver(driver_id),
    constructor_id VARCHAR(50) REFERENCES warehouse.dim_constructor(constructor_id),
    grid_position INT,
    finish_position INT,
    position_text VARCHAR(10),
    points DECIMAL(6,2),
    laps INT,
    race_time VARCHAR(30),
    fastest_lap_time VARCHAR(20),
    fastest_lap_speed VARCHAR(20),
    status_id INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.fact_driver_standings (
    standing_id INT PRIMARY KEY,
    race_id INT REFERENCES warehouse.dim_race(race_id),
    driver_id VARCHAR(50) REFERENCES warehouse.dim_driver(driver_id),
    points DECIMAL(8,2),
    position INT,
    wins INT
);

CREATE TABLE IF NOT EXISTS warehouse.fact_constructor_standings (
    standing_id INT PRIMARY KEY,
    race_id INT REFERENCES warehouse.dim_race(race_id),
    constructor_id VARCHAR(50) REFERENCES warehouse.dim_constructor(constructor_id),
    points DECIMAL(8,2),
    position INT,
    wins INT
);

-- ============================================================
-- CORE SCHEMA: STREAMING
-- ============================================================

CREATE TABLE IF NOT EXISTS core.streaming_lap_events (
    event_id SERIAL PRIMARY KEY,
    race_id INT,
    driver_id VARCHAR(50),
    lap INT,
    position INT,
    lap_time VARCHAR(20),
    milliseconds INT,
    validation_status VARCHAR(20) DEFAULT 'VALID',
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- EDUCATIONAL SNOWFLAKE VIEWS (Phase 21)
-- ============================================================

CREATE OR REPLACE VIEW warehouse.dim_nationality AS
SELECT DISTINCT nationality FROM warehouse.dim_driver WHERE nationality IS NOT NULL;

CREATE OR REPLACE VIEW warehouse.dim_country AS
SELECT DISTINCT country FROM warehouse.dim_circuit WHERE country IS NOT NULL;

-- ============================================================
-- OLAP ANALYTICAL VIEWS (Data Cube) (Phase 22)
-- ============================================================

CREATE OR REPLACE VIEW warehouse.v_driver_championship AS
SELECT
    dd.driver_name,
    dd.nationality,
    dd.code,
    dr.year,
    SUM(fr.points) AS total_points,
    SUM(CASE WHEN fr.finish_position = 1 THEN 1 ELSE 0 END) AS wins,
    COUNT(fr.result_id) AS races_entered
FROM warehouse.fact_race_results fr
JOIN warehouse.dim_driver dd ON fr.driver_id = dd.driver_id
JOIN warehouse.dim_race dr ON fr.race_id = dr.race_id
GROUP BY dd.driver_name, dd.nationality, dd.code, dr.year;

CREATE OR REPLACE VIEW warehouse.v_constructor_championship AS
SELECT
    dc.name AS constructor_name,
    dc.nationality,
    dr.year,
    SUM(fr.points) AS total_points,
    SUM(CASE WHEN fr.finish_position = 1 THEN 1 ELSE 0 END) AS wins,
    COUNT(fr.result_id) AS races
FROM warehouse.fact_race_results fr
JOIN warehouse.dim_constructor dc ON fr.constructor_id = dc.constructor_id
JOIN warehouse.dim_race dr ON fr.race_id = dr.race_id
GROUP BY dc.name, dc.nationality, dr.year;
