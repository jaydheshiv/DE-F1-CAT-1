from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_f1_data(**kwargs):
    # Pull the latest race results from Ergast API
    url = "http://ergast.com/api/f1/current/last/results.json"
    response = requests.get(url)
    data = response.json()
    
    # Pass data to the next task using XCom
    kwargs['ti'].xcom_push(key='raw_f1_data', value=data)

def transform_and_load_f1_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(key='raw_f1_data', task_ids='extract_f1_data')
    
    race_data = raw_data['MRData']['RaceTable']['Races'][0]
    race_id = f"{race_data['season']}_{race_data['round']}"
    
    # 1. Dimension: Circuit
    circuit = race_data['Circuit']
    circuit_record = (
        circuit['circuitId'], circuit['circuitName'], 
        circuit['Location']['locality'], circuit['Location']['country']
    )
    
    # 2. Dimension: Race
    race_record = (
        race_id, int(race_data['season']), int(race_data['round']), 
        circuit['circuitId'], race_data['raceName'], race_data['date']
    )
    
    # Extract Driver, Constructor, and Fact Data
    driver_records = []
    constructor_records = []
    fact_records = []
    
    for result in race_data['Results']:
        driver = result['Driver']
        driver_records.append((
            driver['driverId'], f"{driver['givenName']} {driver['familyName']}", 
            driver.get('nationality', ''), driver.get('dateOfBirth', '1900-01-01')
        ))
        
        constructor = result['Constructor']
        constructor_records.append((
            constructor['constructorId'], constructor['name'], constructor.get('nationality', '')
        ))
        
        result_id = f"{race_id}_{driver['driverId']}"
        fact_records.append((
            result_id, race_id, driver['driverId'], constructor['constructorId'],
            int(result['grid']), int(result.get('position', 0)), 
            float(result['points']), int(result.get('laps', 0)), 
            result.get('FastestLap', {}).get('Time', {}).get('time', 'N/A')
        ))
        
    # Remove duplicates from dimensions
    driver_records = list(set(driver_records))
    constructor_records = list(set(constructor_records))
    
    # 3. Load to Postgres with Idempotency (UPSERT)
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Load Circuit
        cursor.execute("""
            INSERT INTO dim_circuit (circuit_id, name, location, country)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (circuit_id) DO UPDATE SET 
            name = EXCLUDED.name, location = EXCLUDED.location, country = EXCLUDED.country;
        """, circuit_record)
        
        # Load Race
        cursor.execute("""
            INSERT INTO dim_race (race_id, year, round, circuit_id, name, date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (race_id) DO UPDATE SET 
            name = EXCLUDED.name, date = EXCLUDED.date;
        """, race_record)
        
        # Load Drivers
        for d in driver_records:
            cursor.execute("""
                INSERT INTO dim_driver (driver_id, driver_name, nationality, dob)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (driver_id) DO UPDATE SET 
                driver_name = EXCLUDED.driver_name;
            """, d)
            
        # Load Constructors
        for c in constructor_records:
            cursor.execute("""
                INSERT INTO dim_constructor (constructor_id, name, nationality)
                VALUES (%s, %s, %s)
                ON CONFLICT (constructor_id) DO UPDATE SET 
                name = EXCLUDED.name;
            """, c)
            
        # Load Fact (Idempotent Merge)
        for f in fact_records:
            cursor.execute("""
                INSERT INTO fact_race_results 
                (result_id, race_id, driver_id, constructor_id, grid_position, finish_position, points, laps, fastest_lap_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (result_id) DO UPDATE SET 
                finish_position = EXCLUDED.finish_position, points = EXCLUDED.points, fastest_lap_time = EXCLUDED.fastest_lap_time;
            """, f)
            
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


with DAG(
    'f1_historical_pipeline',
    default_args=default_args,
    description='Extracts F1 data, transforms to Star Schema, loads to Postgres',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_f1_data',
        python_callable=extract_f1_data,
        provide_context=True
    )

    transform_load_task = PythonOperator(
        task_id='transform_and_load_f1_data',
        python_callable=transform_and_load_f1_data,
        provide_context=True
    )

    extract_task >> transform_load_task
