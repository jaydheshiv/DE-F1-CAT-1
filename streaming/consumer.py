"""
F1 Lap Event Consumer
Consumes lap events from f1-lap-events topic.
Validation rules:
  - driver_id must not be NULL
  - milliseconds must be > 0
Valid   → streaming_lap_events table
Invalid → pipeline_errors table (Dead Letter Queue)
"""
import json, os, time, logging
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [F1-CONSUMER] %(message)s")

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC         = os.environ.get("KAFKA_TOPIC", "f1-lap-events")
GROUP_ID      = "f1-lap-consumer-group"

def get_db():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "labdb"),
        user=os.environ.get("POSTGRES_USER", "labadmin"),
        password=os.environ.get("POSTGRES_PASSWORD", "labpassword"),
    )


def validate_lap_event(event: dict) -> tuple[bool, str]:
    """Returns (is_valid, error_message)."""
    if not event.get("driver_id"):
        return False, "NULL_DRIVER_ID: driver_id is missing or null"
    ms = event.get("milliseconds")
    if ms is not None and ms < 0:
        return False, f"NEGATIVE_MILLISECONDS: lap time is {ms}ms"
    if ms is not None and ms > 300000:   # > 5 minutes — extreme outlier
        return False, f"OUTLIER_LAP_TIME: {ms}ms exceeds 5 minutes"
    return True, ""


def insert_valid_event(cur, event: dict):
    cur.execute("""
        INSERT INTO streaming_lap_events
            (race_id, driver_id, lap, position, lap_time, milliseconds, validation_status)
        VALUES (%s, %s, %s, %s, %s, %s, 'VALID')
    """, (
        event.get("race_id"),
        event.get("driver_id"),
        event.get("lap"),
        event.get("position"),
        event.get("lap_time"),
        event.get("milliseconds"),
    ))


def insert_error(cur, event: dict, error_msg: str):
    error_type = error_msg.split(":")[0] if ":" in error_msg else "VALIDATION_ERROR"
    cur.execute("""
        INSERT INTO pipeline_errors (source_id, error_type, error_message, raw_payload)
        VALUES (%s, %s, %s, %s)
    """, (
        f"LAP-{event.get('race_id','?')}-{event.get('driver_id','NULL')}-{event.get('lap','?')}",
        error_type,
        error_msg,
        json.dumps(event),
    ))


def main():
    time.sleep(25)  # Wait for Kafka + Postgres

    # ── Connect to PostgreSQL ──────────────────────────────
    retries = 8
    conn = None
    while retries > 0:
        try:
            conn = get_db()
            logging.info("Connected to PostgreSQL!")
            break
        except Exception as e:
            logging.warning(f"Postgres not ready: {e}. Retrying…")
            retries -= 1
            time.sleep(5)
    if not conn:
        logging.error("Could not connect to PostgreSQL. Exiting.")
        return

    # ── Connect to Kafka ──────────────────────────────────
    retries = 8
    consumer = None
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_SERVERS,
                group_id=GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=60000,  # Stop after 60s of silence
            )
            logging.info(f"Subscribed to topic: {TOPIC}")
            break
        except KafkaError as e:
            logging.warning(f"Kafka not ready: {e}. Retrying ({retries} left)…")
            retries -= 1
            time.sleep(5)

    if not consumer:
        logging.error("Could not connect to Kafka. Exiting.")
        conn.close()
        return

    # ── Consume messages ──────────────────────────────────
    valid_count = 0
    error_count = 0
    try:
        for msg in consumer:
            event = msg.value
            is_valid, error_msg = validate_lap_event(event)
            try:
                cur = conn.cursor()
                if is_valid:
                    insert_valid_event(cur, event)
                    conn.commit()
                    valid_count += 1
                    logging.info(
                        f"✅ VALID  | race={event.get('race_id')} "
                        f"drv={event.get('driver_code','?'):4s} "
                        f"lap={event.get('lap')} pos={event.get('position')}"
                    )
                else:
                    insert_error(cur, event, error_msg)
                    conn.commit()
                    error_count += 1
                    logging.warning(f"❌ DLQ    | {error_msg}")
                cur.close()
            except Exception as e:
                conn.rollback()
                logging.error(f"DB error processing event: {e}")
    except Exception as e:
        logging.info(f"Consumer stopped: {e}")
    finally:
        consumer.close()
        conn.close()
        logging.info(
            f"Consumer finished. Valid={valid_count} | DLQ={error_count}"
        )


if __name__ == "__main__":
    main()
