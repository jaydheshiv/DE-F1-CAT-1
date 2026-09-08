"""
F1DB ETL Pipeline — Data Engineering Lab
Reads Formula 1 CSVs mounted at /app/f1db/ and loads them into
the F1 Star Schema in PostgreSQL with full idempotency (ON CONFLICT).
"""
from datetime import datetime, timedelta
import os
import csv
import psycopg2
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

# ──────────────────────────────────────────
# DB connection helper
# ──────────────────────────────────────────
def _get_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "labdb"),
        user=os.environ.get("POSTGRES_USER", "labadmin"),
        password=os.environ.get("POSTGRES_PASSWORD", "labpassword"),
    )

CSV_DIR = "/app/f1db"

# ──────────────────────────────────────────
# Loader helpers
# ──────────────────────────────────────────
def _nullify(val):
    """Convert Ergast-style '\\N' and empty strings to None."""
    if val in ("\\N", "", None):
        return None
    return val


def load_drivers(**context):
    path = os.path.join(CSV_DIR, "drivers.csv")
    conn = _get_conn()
    count = 0
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cur = conn.cursor()
            for row in reader:
                try:
                    dob = _nullify(row["dob"])
                    cur.execute("""
                        INSERT INTO dim_driver (driver_id, driver_name, code, number, nationality, dob, wiki_url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (driver_id) DO UPDATE SET
                            driver_name  = EXCLUDED.driver_name,
                            code         = EXCLUDED.code,
                            number       = EXCLUDED.number,
                            nationality  = EXCLUDED.nationality
                    """, (
                        row["driverRef"],
                        f"{row['forename']} {row['surname']}",
                        _nullify(row["code"]),
                        int(row["number"]) if _nullify(row["number"]) else None,
                        _nullify(row["nationality"]),
                        dob,
                        _nullify(row["url"]),
                    ))
                    count += 1
                except Exception:
                    pass
            conn.commit()
    finally:
        conn.close()
    print(f"[load_drivers] Loaded {count} drivers")
    context["ti"].xcom_push(key="rows_loaded", value=count)


def load_constructors(**context):
    path = os.path.join(CSV_DIR, "constructors.csv")
    conn = _get_conn()
    count = 0
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cur = conn.cursor()
            for row in reader:
                cur.execute("""
                    INSERT INTO dim_constructor (constructor_id, name, nationality, wiki_url)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (constructor_id) DO UPDATE SET
                        name        = EXCLUDED.name,
                        nationality = EXCLUDED.nationality
                """, (
                    row["constructorRef"],
                    row["name"],
                    _nullify(row["nationality"]),
                    _nullify(row["url"]),
                ))
                count += 1
            conn.commit()
    finally:
        conn.close()
    print(f"[load_constructors] Loaded {count} constructors")
    context["ti"].xcom_push(key="rows_loaded", value=count)


def load_circuits(**context):
    path = os.path.join(CSV_DIR, "circuits.csv")
    conn = _get_conn()
    count = 0
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cur = conn.cursor()
            for row in reader:
                cur.execute("""
                    INSERT INTO dim_circuit (circuit_id, name, location, country, lat, lng, alt, wiki_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (circuit_id) DO UPDATE SET
                        name     = EXCLUDED.name,
                        location = EXCLUDED.location,
                        country  = EXCLUDED.country
                """, (
                    row["circuitRef"],
                    row["name"],
                    _nullify(row["location"]),
                    _nullify(row["country"]),
                    float(row["lat"]) if _nullify(row["lat"]) else None,
                    float(row["lng"]) if _nullify(row["lng"]) else None,
                    int(row["alt"]) if _nullify(row["alt"]) else None,
                    _nullify(row["url"]),
                ))
                count += 1
            conn.commit()
    finally:
        conn.close()
    print(f"[load_circuits] Loaded {count} circuits")
    context["ti"].xcom_push(key="rows_loaded", value=count)


def load_races(**context):
    path = os.path.join(CSV_DIR, "races.csv")
    circuits_path = os.path.join(CSV_DIR, "circuits.csv")
    
    # Build circuitId -> circuitRef map
    cir_map = {}
    with open(circuits_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cir_map[row["circuitId"]] = row["circuitRef"]

    conn = _get_conn()
    count = 0
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cur = conn.cursor()
            for row in reader:
                cir_ref = cir_map.get(row["circuitId"])
                if not cir_ref:
                    continue
                try:
                    cur.execute("""
                        INSERT INTO dim_race (race_id, year, round, circuit_id, name, race_date, wiki_url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (race_id) DO UPDATE SET
                            name      = EXCLUDED.name,
                            race_date = EXCLUDED.race_date
                    """, (
                        int(row["raceId"]),
                        int(row["year"]),
                        int(row["round"]),
                        cir_ref,
                        row["name"],
                        _nullify(row["date"]),
                        _nullify(row["url"]),
                    ))
                    count += 1
                except Exception as e:
                    pass
            conn.commit()
    finally:
        conn.close()
    print(f"[load_races] Loaded {count} races")
    context["ti"].xcom_push(key="rows_loaded", value=count)


def load_results(**context):
    """Load race results — maps constructorId to constructorRef via in-memory lookup."""
    results_path = os.path.join(CSV_DIR, "results.csv")
    constructors_path = os.path.join(CSV_DIR, "constructors.csv")
    drivers_path = os.path.join(CSV_DIR, "drivers.csv")
    
    # Build id → ref maps
    con_map = {}
    with open(constructors_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            con_map[row["constructorId"]] = row["constructorRef"]
    
    drv_map = {}
    with open(drivers_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            drv_map[row["driverId"]] = row["driverRef"]
    
    conn = _get_conn()
    count = 0
    try:
        with open(results_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cur = conn.cursor()
            for row in reader:
                try:
                    driver_ref = drv_map.get(row["driverId"])
                    con_ref    = con_map.get(row["constructorId"])
                    if not driver_ref or not con_ref:
                        continue
                    pos = _nullify(row["position"])
                    cur.execute("""
                        INSERT INTO fact_race_results
                            (result_id, race_id, driver_id, constructor_id,
                             grid_position, finish_position, position_text,
                             points, laps, race_time,
                             fastest_lap_time, fastest_lap_speed, status_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (result_id) DO UPDATE SET
                            finish_position  = EXCLUDED.finish_position,
                            points           = EXCLUDED.points,
                            fastest_lap_time = EXCLUDED.fastest_lap_time
                    """, (
                        int(row["resultId"]),
                        int(row["raceId"]),
                        driver_ref,
                        con_ref,
                        int(row["grid"]) if _nullify(row["grid"]) else None,
                        int(pos) if pos else None,
                        _nullify(row["positionText"]),
                        float(row["points"]) if _nullify(row["points"]) else 0,
                        int(row["laps"]) if _nullify(row["laps"]) else None,
                        _nullify(row["time"]),
                        _nullify(row["fastestLapTime"]),
                        _nullify(row["fastestLapSpeed"]),
                        int(row["statusId"]) if _nullify(row["statusId"]) else None,
                    ))
                    count += 1
                except Exception:
                    pass
            conn.commit()
    finally:
        conn.close()
    print(f"[load_results] Loaded {count} results")
    context["ti"].xcom_push(key="rows_loaded", value=count)


def load_driver_standings(**context):
    path = os.path.join(CSV_DIR, "driver_standings.csv")
    drivers_path = os.path.join(CSV_DIR, "drivers.csv")
    drv_map = {}
    with open(drivers_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            drv_map[row["driverId"]] = row["driverRef"]

    conn = _get_conn()
    count = 0
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cur = conn.cursor()
            for row in reader:
                driver_ref = drv_map.get(row["driverId"])
                if not driver_ref:
                    continue
                try:
                    cur.execute("""
                        INSERT INTO fact_driver_standings (standing_id, race_id, driver_id, points, position, wins)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (standing_id) DO UPDATE SET
                            points   = EXCLUDED.points,
                            position = EXCLUDED.position,
                            wins     = EXCLUDED.wins
                    """, (
                        int(row["driverStandingsId"]),
                        int(row["raceId"]),
                        driver_ref,
                        float(row["points"]) if _nullify(row["points"]) else 0,
                        int(row["position"]) if _nullify(row["position"]) else None,
                        int(row["wins"]) if _nullify(row["wins"]) else 0,
                    ))
                    count += 1
                except Exception:
                    pass
            conn.commit()
    finally:
        conn.close()
    print(f"[load_driver_standings] Loaded {count} standings rows")
    context["ti"].xcom_push(key="rows_loaded", value=count)


def load_constructor_standings(**context):
    path = os.path.join(CSV_DIR, "constructor_standings.csv")
    constructors_path = os.path.join(CSV_DIR, "constructors.csv")
    con_map = {}
    with open(constructors_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            con_map[row["constructorId"]] = row["constructorRef"]

    conn = _get_conn()
    count = 0
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            cur = conn.cursor()
            for row in reader:
                con_ref = con_map.get(row["constructorId"])
                if not con_ref:
                    continue
                try:
                    cur.execute("""
                        INSERT INTO fact_constructor_standings (standing_id, race_id, constructor_id, points, position, wins)
                        VALUES (%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (standing_id) DO UPDATE SET
                            points   = EXCLUDED.points,
                            position = EXCLUDED.position,
                            wins     = EXCLUDED.wins
                    """, (
                        int(row["constructorStandingsId"]),
                        int(row["raceId"]),
                        con_ref,
                        float(row["points"]) if _nullify(row["points"]) else 0,
                        int(row["position"]) if _nullify(row["position"]) else None,
                        int(row["wins"]) if _nullify(row["wins"]) else 0,
                    ))
                    count += 1
                except Exception:
                    pass
            conn.commit()
    finally:
        conn.close()
    print(f"[load_constructor_standings] Loaded {count} standings rows")
    context["ti"].xcom_push(key="rows_loaded", value=count)


def log_etl_run(**context):
    total = 0
    for task_id in ["load_drivers", "load_constructors", "load_circuits", "load_races",
                    "load_results", "load_driver_standings", "load_constructor_standings"]:
        rows = context["ti"].xcom_pull(task_ids=task_id, key="rows_loaded") or 0
        total += rows
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO etl_run_log (load_type, rows_extracted, rows_loaded, status, completed_at) "
                "VALUES (%s,%s,%s,%s,NOW())",
                ("F1_CSV_FULL", total, total, "SUCCESS")
            )
        conn.commit()
        print(f"[{context['ds']}] ETL complete. Total rows: {total}")
    finally:
        conn.close()


# ──────────────────────────────────────────
# DAG Definition
# ──────────────────────────────────────────
default_args = {
    "owner": "data_engineering_lab",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 9),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "f1_etl_pipeline",
    default_args=default_args,
    description="F1DB CSV → PostgreSQL ETL (Drivers, Circuits, Races, Results, Standings)",
    schedule=timedelta(days=7),
    catchup=False,
    tags=["data_engineering", "f1", "etl", "star_schema"],
) as dag:

    start = EmptyOperator(task_id="start_pipeline")

    t_drivers      = PythonOperator(task_id="load_drivers",              python_callable=load_drivers)
    t_constructors = PythonOperator(task_id="load_constructors",         python_callable=load_constructors)
    t_circuits     = PythonOperator(task_id="load_circuits",             python_callable=load_circuits)
    t_races        = PythonOperator(task_id="load_races",                python_callable=load_races)
    t_results      = PythonOperator(task_id="load_results",              python_callable=load_results)
    t_drv_stand    = PythonOperator(task_id="load_driver_standings",     python_callable=load_driver_standings)
    t_con_stand    = PythonOperator(task_id="load_constructor_standings", python_callable=load_constructor_standings)

    validate = SQLExecuteQueryOperator(
        task_id="validate_data_quality",
        conn_id="postgres_default",
        sql="""
            SELECT
                (SELECT COUNT(*) FROM dim_driver      WHERE driver_name IS NULL) AS null_drivers,
                (SELECT COUNT(*) FROM dim_circuit     WHERE name IS NULL)        AS null_circuits,
                (SELECT COUNT(*) FROM fact_race_results)                         AS total_results;
        """,
    )

    log_run = PythonOperator(task_id="log_etl_run", python_callable=log_etl_run)
    end     = EmptyOperator(task_id="pipeline_success")

    # Dependency chain
    start >> [t_drivers, t_constructors, t_circuits]
    t_circuits >> t_races
    [t_drivers, t_constructors, t_races] >> t_results
    [t_drivers, t_races] >> t_drv_stand
    [t_constructors, t_races] >> t_con_stand
    [t_results, t_drv_stand, t_con_stand] >> validate >> log_run >> end
