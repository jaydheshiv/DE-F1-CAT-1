# F1 Data Engineering Lab — Final Project Report

## 1. Project Overview
The **F1 Data Engineering Lab** is a comprehensive, full-stack data engineering project designed to orchestrate, transform, and analyze Formula 1 historical and real-time data. The project simulates a real-world enterprise data platform, implementing critical data engineering concepts including ETL/ELT pipelines, Change Data Capture (CDC), Dimensional Modeling (Star Schema), batch orchestration, and pipeline resiliency.

The entire infrastructure is containerized using Docker and features a custom-built React frontend dashboard for monitoring and triggering pipeline jobs, supported by a FastAPI backend.

---

## 2. Technology Stack
*   **Orchestration:** Apache Airflow
*   **Stream Processing (CDC):** Apache Kafka & Zookeeper
*   **Database / Data Warehouse:** PostgreSQL
*   **Backend / API:** FastAPI (Python), Pandas, psycopg2
*   **Frontend UI:** React.js, Vite, Recharts, CSS3
*   **Infrastructure:** Docker & Docker Compose

---

## 3. Architecture & Implementation by Phase

### Week 1: Data Collection, Preprocessing & EDA
*   **Objective:** Ingest raw F1 CSV files and perform Exploratory Data Analysis (EDA).
*   **Implementation:** 
    *   Developed a dynamic EDA engine using Pandas to instantly calculate row/column counts, data types, null percentages, statistical summaries (mean, median, standard deviation), and identify outliers using the IQR method.
    *   Engineered a seamless API capable of handling missing data (`NaN` serialization handling) and generating dynamic histogram bins.

### Week 2: Core Data Pipeline (ETL) & Streaming
*   **Objective:** Build batch ETL capabilities and real-time streaming.
*   **Implementation (Batch):** 
    *   Implemented Full and Incremental data loads from raw CSVs into PostgreSQL.
    *   Handled data cleansing (e.g., converting `\N` to nulls) and type coercion during the transform phase.
*   **Implementation (Streaming CDC):**
    *   Built a Kafka Producer that simulates a live F1 race by emitting lap time telemetry at 1-second intervals.
    *   Built a Kafka Consumer to listen to the `f1-lap-events` topic and insert records into a PostgreSQL streaming table in real-time.

### Week 3: Data Architecture & Schema Design
*   **Objective:** Transform highly normalized OLTP data into an optimized OLAP warehouse.
*   **Implementation:** 
    *   Designed a **Star Schema** centered around a `fact_race_results` table.
    *   Created dimension tables: `dim_driver`, `dim_constructor`, `dim_circuit`, and `dim_race`.
    *   This denormalization enables blazing-fast aggregation queries for the frontend data cubes (e.g., "Points by Driver", "Fastest Drivers").

### Week 4: Resilient API Batch Pipeline
*   **Objective:** Extract live data from external web APIs with fallback mechanisms.
*   **Implementation:** 
    *   Configured a pipeline to extract the latest race results from the live Ergast F1 API (with Jolpi API as a fallback).
    *   Implemented validation steps before inserting the payload into an `etl_staging` table.
    *   Introduced the concept of atomic database transactions to simulate failure and ensure no partial data is ever committed to the database.

### Week 5: Production-Ready Pipelines & Orchestration
*   **Objective:** Ensure pipeline resiliency, scheduling, and error handling.
*   **Implementation:**
    *   **Airflow Orchestration:** Built a comprehensive Airflow DAG (`f1_etl_pipeline`) that executes the entire warehouse load sequentially (Drivers -> Circuits -> Constructors -> Races -> Results -> Validations).
    *   **Idempotency:** Implemented `ON CONFLICT DO UPDATE` (UPSERT) logic across the warehouse so pipelines can be run infinitely without duplicating data.
    *   **Dead Letter Queue (DLQ):** Added strict validation to the Kafka consumer. Invalid lap events (e.g., NULL driver IDs, negative lap times) are caught and routed to a `pipeline_errors` (DLQ) table instead of crashing the stream.

---

## 4. UI/UX & Dashboarding
A premium, dark-themed React dashboard was built to act as the "Control Center" for the data platform. It features:
*   Real-time Kafka streaming feeds.
*   Interactive EDA tables and statistical charts (Recharts).
*   Visual Airflow DAG triggers via HTTP REST API.
*   Staging and DLQ monitoring panels.

---

## 5. Conclusion
This project successfully demonstrates the end-to-end lifecycle of data engineering. By combining batch processing (Airflow) with real-time streaming (Kafka), modeling data for analytics (Star Schema), and implementing strict production safeguards (Idempotency, Atomicity, DLQ), the F1 Data Engineering Lab serves as a highly robust, scalable, and resilient modern data architecture.
