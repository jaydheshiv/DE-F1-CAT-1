from datetime import datetime
import os, json, io, csv, traceback
import requests
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="F1 Data Engineering Lab API — 22MDCEL10")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CSV_DIR = os.environ.get("F1_CSV_DIR", "/app/f1db")

# ──────────────────────────────────────────────
# DB Helper
# ──────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "labdb"),
        user=os.environ.get("POSTGRES_USER", "labadmin"),
        password=os.environ.get("POSTGRES_PASSWORD", "labpassword"),
    )

# ──────────────────────────────────────────────
# HEALTH
# ──────────────────────────────────────────────
@app.get("/api/health")
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat(), "dataset": "F1DB"}

# ══════════════════════════════════════════════
# WEEK 1 — EDA (works for any CSV)
# ══════════════════════════════════════════════

def _compute_eda(df: pd.DataFrame) -> dict:
    summary = []
    for col in df.columns:
        s = df[col]
        dtype = str(s.dtype)
        null_count = int(s.isnull().sum())
        null_pct = round(null_count / len(s) * 100, 1) if len(s) > 0 else 0
        unique = int(s.nunique())
        entry = {
            "column": col, "dtype": dtype,
            "null_count": null_count, "null_pct": null_pct,
            "unique": unique,
        }
        if pd.api.types.is_numeric_dtype(s):
            def safe_flt(v):
                return None if pd.isna(v) else round(float(v), 2)
            
            q1, q3 = s.quantile(0.25), s.quantile(0.75)
            iqr = q3 - q1
            outliers = int(((s < q1 - 1.5 * iqr) | (s > q3 + 1.5 * iqr)).sum())
            entry.update({
                "min": safe_flt(s.min()),
                "max": safe_flt(s.max()),
                "mean": safe_flt(s.mean()),
                "median": safe_flt(s.median()),
                "std": safe_flt(s.std()),
                "outliers": outliers,
            })
        else:
            top_val = s.value_counts().idxmax() if not s.dropna().empty else None
            entry.update({"top_value": str(top_val) if top_val is not None else None})
        summary.append(entry)

    cat_cols = df.select_dtypes(include=["object"]).columns.tolist()
    distributions = {}
    for col in cat_cols[:4]:
        vc = df[col].value_counts().head(10)
        distributions[col] = [{"label": str(k), "count": int(v)} for k, v in vc.items()]

    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    histograms = {}
    for col in num_cols[:4]:
        clean_col = df[col].dropna()
        if clean_col.empty:
            continue
        counts, edges = np.histogram(clean_col, bins=10)
        histograms[col] = [
            {"bin": f"{round(float(edges[i]),1)}–{round(float(edges[i+1]),1)}", "count": int(counts[i])}
            for i in range(len(counts))
        ]

    return {
        "rows": len(df), "columns": len(df.columns),
        "column_names": df.columns.tolist(),
        "summary": summary,
        "distributions": distributions,
        "histograms": histograms,
        "missing_overview": [{"column": c, "missing": int(df[c].isnull().sum())} for c in df.columns],
    }


@app.post("/api/week1/eda/upload")
async def eda_upload(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        fname    = (file.filename or "upload").lower()

        # ── Parse: support CSV, XLS, XLSX ──
        if fname.endswith((".xls", ".xlsx")):
            df = pd.read_excel(io.BytesIO(contents), na_values=["\\N", ""])
            file_type = "Excel"
        else:
            df = pd.read_csv(io.BytesIO(contents), na_values=["\\N", ""])
            file_type = "CSV"

        eda = _compute_eda(df)
        eda["file_type"]  = file_type
        return eda
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"EDA failed: {e}")


F1_TABLES = {
    "drivers":              "drivers.csv",
    "races":                "races.csv",
    "results":              "results.csv",
    "circuits":             "circuits.csv",
    "constructors":         "constructors.csv",
    "driver_standings":     "driver_standings.csv",
    "constructor_standings":"constructor_standings.csv",
}

@app.get("/api/week1/eda/f1/{table_name}")
def eda_f1_table(table_name: str):
    if table_name not in F1_TABLES:
        raise HTTPException(status_code=404, detail=f"Unknown table. Choose from: {list(F1_TABLES.keys())}")
    path = os.path.join(CSV_DIR, F1_TABLES[table_name])
    try:
        df = pd.read_csv(path, na_values=["\\N", ""])
        # For large files (lap_times), sample 10K rows
        if len(df) > 10000:
            df = df.sample(10000, random_state=42)
        return _compute_eda(df)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"CSV not found: {path}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ══════════════════════════════════════════════
# WEEK 2 — ETL Pipeline
# ══════════════════════════════════════════════

@app.get("/api/week2/etl/runs")
def get_etl_runs():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM etl_run_log ORDER BY started_at DESC LIMIT 10")
            return cur.fetchall()
    finally:
        conn.close()


@app.post("/api/week2/etl/full-load")
def trigger_full_load():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_race_results")
            before = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO etl_run_log (load_type, rows_extracted, rows_loaded, status, completed_at) "
                "VALUES (%s,%s,%s,%s,NOW())",
                ("FULL", before, before, "SUCCESS")
            )
        conn.commit()
        return {"load_type": "FULL", "rows_loaded": before, "status": "SUCCESS",
                "message": f"Full load logged. {before} results in fact_race_results."}
    except Exception as e:
        conn.rollback(); raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/api/week2/etl/incremental-load")
def trigger_incremental_load():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            import random
            # SIMULATION: Randomly touch timestamps of 1 to 15 rows to pretend new data just arrived!
            cur.execute(f"UPDATE warehouse.fact_race_results SET updated_at = NOW() + INTERVAL '1 second' WHERE result_id IN (SELECT result_id FROM warehouse.fact_race_results ORDER BY RANDOM() LIMIT {random.randint(1, 15)})")

            cur.execute("SELECT MAX(started_at) FROM etl_run_log WHERE status='SUCCESS'")
            last_run = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM warehouse.fact_race_results WHERE updated_at > %s",
                        (last_run or "2000-01-01",))
            new_rows = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO etl_run_log (load_type, rows_extracted, rows_loaded, status, completed_at) "
                "VALUES (%s,%s,%s,%s,NOW())",
                ("INCREMENTAL", new_rows, new_rows, "SUCCESS")
            )
        conn.commit()
        return {"load_type": "INCREMENTAL", "rows_loaded": new_rows, "status": "SUCCESS",
                "since": str(last_run), "message": f"Incremental load: {new_rows} new results since last run."}
    except Exception as e:
        conn.rollback(); raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.get("/api/week2/kafka/events")
def get_lap_events():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT sle.event_id, sle.race_id, sle.driver_id, dd.code AS driver_code,
                       sle.lap, sle.position, sle.lap_time, sle.milliseconds,
                       sle.validation_status, sle.ingested_at
                FROM streaming_lap_events sle
                LEFT JOIN dim_driver dd ON sle.driver_id = dd.driver_id
                ORDER BY sle.ingested_at DESC LIMIT 20
            """)
            return cur.fetchall()
    finally:
        conn.close()

# ══════════════════════════════════════════════
# WEEK 3 — Schema Design (F1 Star Schema)
# ══════════════════════════════════════════════

@app.get("/api/week3/dimensions/drivers")
def get_dim_drivers(limit: int = Query(default=20, le=100)):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM dim_driver ORDER BY driver_name LIMIT %s", (limit,))
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/api/week3/dimensions/circuits")
def get_dim_circuits(limit: int = Query(default=20, le=100)):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM dim_circuit ORDER BY country, name LIMIT %s", (limit,))
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/api/week3/dimensions/constructors")
def get_dim_constructors(limit: int = Query(default=20, le=100)):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM dim_constructor ORDER BY name LIMIT %s", (limit,))
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/api/week3/dimensions/races")
def get_dim_races(limit: int = Query(default=20, le=100)):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT r.race_id, r.year, r.round, r.name, r.race_date, c.name AS circuit_name, c.country
                FROM dim_race r JOIN dim_circuit c ON r.circuit_id = c.circuit_id
                ORDER BY r.year DESC, r.round DESC LIMIT %s
            """, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/api/week3/fact/results")
def get_fact_results(limit: int = Query(default=20, le=100)):
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    fr.result_id,
                    dr.year, dr.round, dr.name AS race_name,
                    dc.circuit_id, dc.country,
                    dd.driver_name, dd.code AS driver_code, dd.nationality AS driver_nat,
                    dcon.name AS constructor_name,
                    fr.grid_position, fr.finish_position, fr.position_text,
                    fr.points, fr.laps, fr.race_time, fr.fastest_lap_time
                FROM fact_race_results fr
                JOIN dim_race        dr   ON fr.race_id        = dr.race_id
                JOIN dim_circuit     dc   ON dr.circuit_id     = dc.circuit_id
                JOIN dim_driver      dd   ON fr.driver_id      = dd.driver_id
                JOIN dim_constructor dcon ON fr.constructor_id = dcon.constructor_id
                ORDER BY dr.year DESC, dr.round DESC, fr.finish_position ASC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/api/week3/olap/query/{query_name}")
def run_olap_query(query_name: str, year: Optional[int] = None):
    queries = {
        "points_by_driver": {
            "sql": """
                SELECT dd.driver_name, dd.code, dd.nationality,
                       SUM(fr.points) AS total_points,
                       SUM(CASE WHEN fr.finish_position=1 THEN 1 ELSE 0 END) AS wins,
                       COUNT(*) AS races
                FROM fact_race_results fr
                JOIN dim_driver dd ON fr.driver_id = dd.driver_id
                JOIN dim_race   dr ON fr.race_id   = dr.race_id
                {where}
                GROUP BY dd.driver_name, dd.code, dd.nationality
                ORDER BY total_points DESC LIMIT 20
            """,
            "year_col": "dr.year"
        },
        "wins_by_constructor": {
            "sql": """
                SELECT dcon.name AS constructor, dcon.nationality,
                       SUM(fr.points) AS total_points,
                       SUM(CASE WHEN fr.finish_position=1 THEN 1 ELSE 0 END) AS wins,
                       COUNT(DISTINCT fr.race_id) AS races
                FROM fact_race_results fr
                JOIN dim_constructor dcon ON fr.constructor_id = dcon.constructor_id
                JOIN dim_race        dr   ON fr.race_id        = dr.race_id
                {where}
                GROUP BY dcon.name, dcon.nationality
                ORDER BY wins DESC LIMIT 15
            """,
            "year_col": "dr.year"
        },
        "races_by_season": {
            "sql": """
                SELECT dr.year, COUNT(DISTINCT dr.race_id) AS race_count,
                       COUNT(DISTINCT fr.driver_id) AS drivers,
                       COUNT(DISTINCT fr.constructor_id) AS constructors,
                       SUM(fr.points) AS total_points_awarded
                FROM fact_race_results fr
                JOIN dim_race dr ON fr.race_id = dr.race_id
                GROUP BY dr.year ORDER BY dr.year DESC LIMIT 20
            """,
            "year_col": None
        },
        "fastest_drivers": {
            "sql": """
                SELECT dd.driver_name, dd.code,
                       COUNT(fr.result_id) AS fastest_laps,
                       MIN(fr.fastest_lap_time) AS best_lap
                FROM fact_race_results fr
                JOIN dim_driver dd ON fr.driver_id = dd.driver_id
                JOIN dim_race   dr ON fr.race_id   = dr.race_id
                WHERE fr.fastest_lap_time IS NOT NULL
                {and_where}
                GROUP BY dd.driver_name, dd.code
                ORDER BY fastest_laps DESC LIMIT 15
            """,
            "year_col": "dr.year",
            "has_where": True
        },
        "results_by_circuit": {
            "sql": """
                SELECT dc.name AS circuit, dc.country,
                       COUNT(fr.result_id) AS total_results,
                       AVG(fr.points) AS avg_points_per_entry
                FROM fact_race_results fr
                JOIN dim_race    dr ON fr.race_id    = dr.race_id
                JOIN dim_circuit dc ON dr.circuit_id = dc.circuit_id
                GROUP BY dc.name, dc.country ORDER BY total_results DESC LIMIT 15
            """,
            "year_col": None
        },
    }

    if query_name not in queries:
        raise HTTPException(status_code=404,
            detail=f"Unknown query: {query_name}. Available: {list(queries.keys())}")

    q_def = queries[query_name]
    sql_template = q_def["sql"]
    year_col = q_def.get("year_col")
    has_where = q_def.get("has_where", False)

    if year and year_col:
        where_clause    = f"WHERE {year_col} = {year}"
        and_where_clause = f"AND {year_col} = {year}"
    else:
        where_clause    = ""
        and_where_clause = ""

    sql = sql_template.format(where=where_clause, and_where=and_where_clause)

    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return {"query_name": query_name, "year_filter": year, "results": cur.fetchall()}
    finally:
        conn.close()


# ══════════════════════════════════════════════
# WEEK 4 — Batch Pipeline (CSV → Transform → Staging)
# ══════════════════════════════════════════════

@app.post("/api/week4/pipeline/run")
def run_batch_pipeline(force_fail: bool = Query(default=False)):
    steps = []
    try:
        # Step 1: Extract — API with Fallback (Resiliency Pattern)
        url = "https://jolpi.ca/ergast/f1/current/last/results.json"
        try:
            # F1 APIs often block default Python user-agents
            headers = {"User-Agent": "F1DataLab/1.0 (StudentProject)"}
            response = requests.get(url, headers=headers, timeout=5)
            response.raise_for_status()
            data = response.json()
            
            races = data.get("MRData", {}).get("RaceTable", {}).get("Races", [])
            if not races:
                raise Exception("No race data found in API response.")
                
            race_data = races[0]
            results = race_data.get("Results", [])
            
            flat_results = []
            for r in results:
                flat_results.append({
                    "result_id": r.get("position"), # Using position as mock id
                    "race_id": race_data.get("round"),
                    "driver_id": r.get("Driver", {}).get("driverId"),
                    "constructor_id": r.get("Constructor", {}).get("constructorId"),
                    "grid": r.get("grid"),
                    "position": r.get("position"),
                    "points": r.get("points"),
                    "laps": r.get("laps")
                })
                
            df = pd.DataFrame(flat_results)
            steps.append({"step": "1. Extract (API)", "status": "SUCCESS",
                           "detail": f"Fetched {len(df)} records from Ergast API (Race: {race_data.get('raceName')})",
                           "sample": df.head(2).to_dict(orient="records")})
                           
        except Exception as e:
            # Resilient Fallback: If API is down or blocks us, extract from local CSV
            path = os.path.join(CSV_DIR, "results.csv")
            df = pd.read_csv(path, na_values=["\\N", ""], nrows=100)
            df = df[["resultId", "raceId", "driverId", "constructorId",
                     "grid", "position", "points", "laps"]].copy()
            df.columns = ["result_id", "race_id", "driver_id", "constructor_id",
                          "grid", "position", "points", "laps"]
            steps.append({"step": "1. Extract (Fallback)", "status": "WARN",
                           "detail": f"API unavailable ({e}). Fallback to results.csv ({len(df)} rows)",
                           "sample": df.head(2).to_dict(orient="records")})

        # Step 2: Transform & Validate
        df_clean = df.copy()
        df_clean["points"] = pd.to_numeric(df_clean["points"], errors="coerce").fillna(0)
        df_clean["loaded_at"] = pd.Timestamp.now().isoformat()
        
        initial_len = len(df_clean)
        df_clean = df_clean.dropna(subset=["driver_id"])
        
        # Replace NaN with None safely across all columns (requires cast to object first)
        df_clean = df_clean.astype(object).where(pd.notnull(df_clean), None)

        steps.append({"step": "2. Transform & Validate", "status": "SUCCESS",
                       "detail": f"Validated schema, dropped {initial_len - len(df_clean)} null records, casted types",
                       "sample": df_clean.head(2).to_dict(orient="records")})

        # Step 3: Load → staging (Atomicity & Idempotency)
        conn = get_db()
        loaded = 0
        try:
            with conn.cursor() as cur:
                for _, row in df_clean.iterrows():
                    cur.execute(
                        """
                        INSERT INTO etl_staging (source_record_id, raw_payload, validation_status) 
                        VALUES (%s,%s,'VALID') 
                        ON CONFLICT (source_record_id) DO UPDATE 
                            SET raw_payload = EXCLUDED.raw_payload,
                                validation_status = 'VALID',
                                ingested_at = NOW()
                        """,
                        (f"F1-API-RES-{row['driver_id']}-{row['race_id']}", json.dumps(row.to_dict()))
                    )
                    loaded += 1
                    
                if force_fail:
                    raise Exception("Simulated mid-transaction failure (Rollback Test)!")
                    
            conn.commit()
            steps.append({"step": "3. Load (Atomic & Idempotent)", "status": "SUCCESS",
                           "detail": f"UPSERTed {loaded} rows into etl_staging via transaction."})
        except Exception as e:
            conn.rollback()
            if force_fail and "Simulated mid-transaction failure" in str(e):
                steps.append({"step": "3. Load (Rollback Demo)", "status": "SUCCESS", 
                              "detail": "✅ Atomic transaction successfully rolled back! No partial data was inserted into the database."})
            else:
                raise Exception(f"Atomic transaction rolled back! Error: {str(e)}")
        finally:
            conn.close()

    except Exception as e:
        steps.append({"step": "Error", "status": "FAILED", "detail": str(e)})

    return {"pipeline": "Week 4 & 5 F1 API Batch Pipeline", "steps": steps}


@app.post("/api/week4/airflow/trigger")
def trigger_airflow_dag():
    """Trigger the f1_etl_pipeline DAG via Airflow REST API."""
    import requests as req, datetime
    airflow_host = os.environ.get("AIRFLOW_HOST", "http://airflow-webserver:8080")
    airflow_user = os.environ.get("AIRFLOW_USER", "admin")
    airflow_pass = os.environ.get("AIRFLOW_PASSWORD", "admin")
    run_id = f"manual__{datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%S')}"
    try:
        resp = req.post(
            f"{airflow_host}/api/v1/dags/f1_etl_pipeline/dagRuns",
            json={"dag_run_id": run_id},
            auth=(airflow_user, airflow_pass),
            timeout=15
        )
        if resp.status_code in (200, 201):
            return {
                "status": "TRIGGERED",
                "run_id": run_id,
                "message": "f1_etl_pipeline DAG triggered successfully! Click the link below to watch it run.",
                "airflow_url": "http://localhost:8080/dags/f1_etl_pipeline/graph"
            }
        else:
            return {
                "status": "ERROR",
                "message": f"Airflow returned HTTP {resp.status_code}: {resp.text[:200]}",
                "airflow_url": "http://localhost:8080/dags/f1_etl_pipeline/graph"
            }
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}


@app.get("/api/week4/airflow/status")
def get_airflow_dag_status():
    """Get the latest run status of the f1_etl_pipeline DAG via Airflow REST API."""
    import requests as req
    airflow_host = os.environ.get("AIRFLOW_HOST", "http://airflow-webserver:8080")
    airflow_user = os.environ.get("AIRFLOW_USER", "admin")
    airflow_pass = os.environ.get("AIRFLOW_PASSWORD", "admin")
    try:
        resp = req.get(
            f"{airflow_host}/api/v1/dags/f1_etl_pipeline/dagRuns?order_by=-execution_date&limit=1",
            auth=(airflow_user, airflow_pass),
            timeout=15
        )
        data = resp.json()
        runs = data.get("dag_runs", [])
        latest = runs[0] if runs else None
        return {
            "dag_id": "f1_etl_pipeline",
            "latest_run": latest,
            "airflow_url": "http://localhost:8080/dags/f1_etl_pipeline/graph"
        }
    except Exception as e:
        return {"dag_id": "f1_etl_pipeline", "latest_run": None, "error": str(e),
                "airflow_url": "http://localhost:8080/dags/f1_etl_pipeline/graph"}


@app.get("/api/week4/staging")
def get_staging():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM etl_staging ORDER BY ingested_at DESC LIMIT 20")
            return cur.fetchall()
    finally:
        conn.close()

# ══════════════════════════════════════════════
# WEEK 5 — Production-Ready Pipelines
# ══════════════════════════════════════════════

@app.post("/api/week5/idempotency/run")
def idempotency_demo():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            for _ in range(2):
                cur.execute("""
                    INSERT INTO idempotency_test (record_id, value, run_count)
                    VALUES ('F1-HAM-2008', 98.0, 1)
                    ON CONFLICT (record_id) DO UPDATE
                        SET value     = EXCLUDED.value,
                            run_count = idempotency_test.run_count + 1,
                            updated_at = NOW()
                """)
            cur.execute("SELECT * FROM idempotency_test WHERE record_id='F1-HAM-2008'")
            row = cur.fetchone()
        conn.commit()
        return {
            "message": "F1-HAM-2008 upserted twice. Only 1 row exists.",
            "record": {"record_id": row[0], "value": float(row[1]),
                       "run_count": row[2], "updated_at": str(row[3])},
            "concept": "ON CONFLICT (record_id) DO UPDATE ensures idempotency."
        }
    except Exception as e:
        conn.rollback(); raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/api/week5/atomicity/demo")
def atomicity_demo(force_fail: bool = False):
    conn = get_db()
    log = []
    try:
        with conn.cursor() as cur:
            log.append("BEGIN transaction")
            cur.execute(
                "INSERT INTO etl_staging (source_record_id, raw_payload, validation_status) "
                "VALUES ('F1-ATOMIC-TEST', '{\"demo\":true}', 'PENDING')"
            )
            log.append("INSERT lap event into staging — OK")
            if force_fail:
                raise Exception("Simulated mid-transaction failure (e.g. constraint violation)!")
            cur.execute(
                "UPDATE etl_staging SET validation_status='VALID' WHERE source_record_id='F1-ATOMIC-TEST'"
            )
            log.append("UPDATE validation_status='VALID' — OK")
        conn.commit()
        log.append("COMMIT — all changes persisted")
        return {"outcome": "COMMITTED", "log": log}
    except Exception as e:
        conn.rollback()
        log.append(f"ERROR: {e}")
        log.append("ROLLBACK — all changes reverted, DB unchanged")
        return {"outcome": "ROLLED BACK", "log": log, "error": str(e)}
    finally:
        conn.close()


@app.get("/api/week5/staging")
def get_validation_staging():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM etl_staging ORDER BY ingested_at DESC LIMIT 20")
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/api/week5/errors")
def get_errors():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM pipeline_errors ORDER BY failed_at DESC LIMIT 15")
            return cur.fetchall()
    finally:
        conn.close()

@app.get("/api/week5/final")
def get_final_results():
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM warehouse.fact_race_results ORDER BY updated_at DESC LIMIT 10")
            return cur.fetchall()
    finally:
        conn.close()


# ══════════════════════════════════════════════
# F1 SUMMARY STATS  (used by frontend hero cards)
# ══════════════════════════════════════════════

@app.get("/api/f1/stats")
def get_f1_stats():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dim_driver")
            drivers = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dim_circuit")
            circuits = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dim_race")
            races = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT year) FROM dim_race")
            seasons = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM fact_race_results")
            results = cur.fetchone()[0]
        return {
            "drivers": drivers, "circuits": circuits,
            "races": races, "seasons": seasons, "results": results
        }
    finally:
        conn.close()


# ══════════════════════════════════════════════════════════════
# PHASE II — LAKEHOUSE ENDPOINTS (Trino-Powered)
# ══════════════════════════════════════════════════════════════

TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8090"))
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.environ.get("TRINO_SCHEMA", "lakehouse")


def _get_trino():
    """Get a Trino DBAPI connection."""
    try:
        import trino as trino_lib
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="Trino client not installed. Install with: pip install trino"
        )
    return trino_lib.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="api",
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


@app.get("/api/v2/lakehouse/health")
def lakehouse_health():
    """Check lakehouse health: Trino connectivity, Iceberg table stats."""
    try:
        conn = _get_trino()
        cursor = conn.cursor()

        tables_info = {}
        for table in ["raw_lap_events", "dead_letter_queue", "cleaned_laps", "agg_driver_race_stats"]:
            try:
                cursor.execute(f"SELECT count(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{table}")
                count = cursor.fetchone()[0]
                tables_info[table] = {"records": count, "status": "healthy"}
            except Exception as e:
                tables_info[table] = {"records": 0, "status": "error", "error": str(e)}

        cursor.close()
        conn.close()

        return {
            "status": "healthy",
            "trino": {"host": TRINO_HOST, "port": TRINO_PORT, "connected": True},
            "tables": tables_info,
            "timestamp": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        return {
            "status": "degraded",
            "trino": {"host": TRINO_HOST, "port": TRINO_PORT, "connected": False, "error": str(e)},
            "tables": {},
            "timestamp": datetime.utcnow().isoformat(),
        }


@app.get("/api/v2/lakehouse/laps")
def lakehouse_laps(
    limit: int = Query(default=100, ge=1, le=1000),
    race_id: Optional[int] = Query(default=None),
    driver_id: Optional[str] = Query(default=None),
):
    """Query lap events from Iceberg cleaned_laps table via Trino."""
    try:
        conn = _get_trino()
        cursor = conn.cursor()

        query = f"SELECT * FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.cleaned_laps WHERE 1=1"
        if race_id:
            query += f" AND race_id = {race_id}"
        if driver_id:
            query += f" AND driver_id = '{driver_id}'"
        query += f" ORDER BY ingested_at DESC LIMIT {limit}"

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        return {"count": len(rows), "data": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Trino query failed: {e}")


@app.get("/api/v2/lakehouse/stats")
def lakehouse_stats(race_id: Optional[int] = Query(default=None)):
    """Query aggregated driver stats from Iceberg via Trino."""
    try:
        conn = _get_trino()
        cursor = conn.cursor()

        query = f"SELECT * FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.agg_driver_race_stats"
        if race_id:
            query += f" WHERE race_id = {race_id}"
        query += " ORDER BY total_laps DESC LIMIT 100"

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        return {"count": len(rows), "data": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Trino query failed: {e}")


@app.get("/api/v2/lakehouse/dlq")
def lakehouse_dlq(limit: int = Query(default=50, ge=1, le=500)):
    """Query dead letter queue from Iceberg via Trino."""
    try:
        conn = _get_trino()
        cursor = conn.cursor()

        cursor.execute(
            f"SELECT * FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.dead_letter_queue "
            f"ORDER BY ingested_at DESC LIMIT {limit}"
        )
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        return {"count": len(rows), "data": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Trino query failed: {e}")


@app.get("/api/v2/lineage")
def get_lineage():
    """Get data lineage graph."""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT lineage_id, source_system, source_table, transform_name,
                       dest_system, dest_table, column_mappings, description,
                       created_at::text, updated_at::text
                FROM audit.data_lineage
                ORDER BY created_at
            """)
            records = cur.fetchall()

        # Generate Mermaid diagram
        lines = ["graph LR"]
        nodes = set()
        for rec in records:
            src = (rec["source_system"] or "").replace(":", "_").replace("-", "_")
            dst = (rec["dest_system"] or "").replace(":", "_").replace("-", "_")
            src_tbl = (rec["source_table"] or "source").replace("-", "_")
            dst_tbl = (rec["dest_table"] or "dest").replace("-", "_")
            transform = (rec["transform_name"] or "").replace("-", "_").replace(" ", "_")

            src_node = f"{src}__{src_tbl}"
            dst_node = f"{dst}__{dst_tbl}"

            if src_node not in nodes:
                lines.append(f'  {src_node}["{rec["source_system"]}/{rec["source_table"] or "*"}"]')
                nodes.add(src_node)
            if dst_node not in nodes:
                lines.append(f'  {dst_node}["{rec["dest_system"]}/{rec["dest_table"]}"]')
                nodes.add(dst_node)
            lines.append(f"  {src_node} -->|{transform}| {dst_node}")

        mermaid = "\n".join(lines)

        return {
            "records": [dict(r) for r in records],
            "count": len(records),
            "mermaid_diagram": mermaid,
        }
    except Exception as e:
        return {"records": [], "count": 0, "mermaid_diagram": "", "error": str(e)}
    finally:
        conn.close()


@app.get("/api/v2/metrics")
def get_pipeline_metrics():
    """Get recent pipeline metrics."""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT pipeline_name, metric_name,
                       AVG(metric_value) as avg_value,
                       MAX(metric_value) as max_value,
                       MIN(metric_value) as min_value,
                       COUNT(*) as sample_count
                FROM audit.pipeline_metrics
                WHERE recorded_at > NOW() - INTERVAL '1 hour'
                GROUP BY pipeline_name, metric_name
                ORDER BY pipeline_name, metric_name
            """)
            aggregated = cur.fetchall()

            cur.execute("""
                SELECT * FROM audit.pipeline_metrics
                WHERE recorded_at > NOW() - INTERVAL '30 minutes'
                ORDER BY recorded_at DESC
                LIMIT 100
            """)
            recent = cur.fetchall()

        return {
            "aggregated": [dict(r) for r in aggregated],
            "recent": [dict(r) for r in recent],
        }
    except Exception as e:
        return {"aggregated": [], "recent": [], "error": str(e)}
    finally:
        conn.close()


@app.get("/api/v2/iceberg/tables")
def get_iceberg_table_registry():
    """Get Iceberg table metadata from the registry."""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT catalog_name, schema_name, table_name, location,
                       partition_spec, last_compaction::text, last_snapshot_expiry::text,
                       snapshot_count, file_count, total_records, updated_at::text
                FROM audit.iceberg_table_registry
                ORDER BY table_name
            """)
            tables = cur.fetchall()
        return {"tables": [dict(r) for r in tables]}
    except Exception as e:
        return {"tables": [], "error": str(e)}
    finally:
        conn.close()


# ── Prometheus Metrics Endpoint ──────────────────────────────
@app.get("/metrics")
def prometheus_metrics():
    """Expose Prometheus metrics for scraping."""
    try:
        from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
        from fastapi.responses import Response
        return Response(
            content=generate_latest(),
            media_type=CONTENT_TYPE_LATEST,
        )
    except ImportError:
        return JSONResponse(
            content={"error": "prometheus_client not installed"},
            status_code=503,
        )