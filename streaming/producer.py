"""
F1 Lap Event Producer
Reads lap_times.csv and streams lap events for the most recent race,
one lap per second, simulating a live F1 race broadcast.
Topic: f1-lap-events
"""
import json, os, time, logging, csv
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [F1-PRODUCER] %(message)s")

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC        = os.environ.get("KAFKA_TOPIC", "f1-lap-events")
CSV_DIR      = os.environ.get("F1_CSV_DIR", "/app/f1db")
DELAY_SEC    = float(os.environ.get("LAP_DELAY_SEC", "1"))

# ── Build driver code lookup (driverId → driverRef) ──────────────────────────
def _build_driver_map():
    drv_map = {}
    drv_path = os.path.join(CSV_DIR, "drivers.csv")
    try:
        with open(drv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                drv_map[row["driverId"]] = {
                    "ref":  row["driverRef"],
                    "code": row["code"] if row["code"] not in ("\\N", "") else row["driverRef"].upper()[:3],
                    "name": f"{row['forename']} {row['surname']}",
                }
    except FileNotFoundError:
        logging.warning(f"drivers.csv not found at {drv_path}")
    return drv_map


def _get_latest_race_laps(drv_map):
    """Read lap_times.csv and return rows for the HIGHEST raceId found."""
    lap_path = os.path.join(CSV_DIR, "lap_times.csv")
    all_rows = []
    try:
        with open(lap_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_rows.append(row)
    except FileNotFoundError:
        logging.error(f"lap_times.csv not found at {lap_path}")
        return []

    if not all_rows:
        return []

    latest_race_id = max(int(r["raceId"]) for r in all_rows)
    race_laps = [r for r in all_rows if int(r["raceId"]) == latest_race_id]
    # Sort by lap number, then position
    race_laps.sort(key=lambda r: (int(r["lap"]), int(r["position"])))

    events = []
    for row in race_laps:
        driver_info = drv_map.get(row["driverId"], {
            "ref": f"driver_{row['driverId']}", "code": "UNK", "name": "Unknown"
        })
        ms = int(row["milliseconds"]) if row["milliseconds"] not in ("\\N", "") else None
        events.append({
            "race_id":      int(row["raceId"]),
            "driver_id":    driver_info["ref"],
            "driver_code":  driver_info["code"],
            "driver_name":  driver_info["name"],
            "lap":          int(row["lap"]),
            "position":     int(row["position"]),
            "lap_time":     row["time"] if row["time"] not in ("\\N", "") else None,
            "milliseconds": ms,
            "event_type":   "LAP_COMPLETE",
        })

    # Inject intentional bad events for DLQ demo
    events.insert(5, {
        "race_id":    latest_race_id, "driver_id": None,
        "driver_code": None, "driver_name": None,
        "lap": 1, "position": 99, "lap_time": None,
        "milliseconds": None, "event_type": "LAP_COMPLETE",
    })
    events.insert(10, {
        "race_id":    latest_race_id, "driver_id": "outlier_driver",
        "driver_code": "OUT", "driver_name": "Outlier Driver",
        "lap": 2, "position": 1, "lap_time": "0:00.000",
        "milliseconds": -999, "event_type": "LAP_COMPLETE",
    })
    return events


def main():
    time.sleep(20)   # Wait for Kafka to be ready

    drv_map = _build_driver_map()
    events  = _get_latest_race_laps(drv_map)

    if not events:
        logging.warning("No lap events to stream. Exiting.")
        return

    race_id = events[0]["race_id"] if events[0]["driver_id"] else "unknown"
    logging.info(f"Streaming {len(events)} lap events for race_id={race_id}")

    # ── Connect to Kafka ──────────────────────────────────────
    retries = 8
    producer = None
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logging.info("Connected to Kafka!")
            break
        except KafkaError as e:
            logging.warning(f"Kafka not ready: {e}. Retrying ({retries} left)…")
            retries -= 1
            time.sleep(5)

    if not producer:
        logging.error("Could not connect to Kafka. Exiting.")
        return

    # ── Stream events ─────────────────────────────────────────
    for event in events:
        try:
            future = producer.send(TOPIC, value=event)
            meta   = future.get(timeout=10)
            status = "❌ BAD" if event["driver_id"] is None or (event["milliseconds"] and event["milliseconds"] < 0) else "✅"
            logging.info(
                f"{status} LAP={event['lap']:3d} | {(event.get('driver_code') or 'NULL'):4s} | "
                f"pos={event['position']} | t={event.get('lap_time','N/A')} | "
                f"partition={meta.partition} offset={meta.offset}"
            )
        except Exception as e:
            logging.error(f"Failed to send event: {e}")
        time.sleep(DELAY_SEC)

    producer.flush()
    producer.close()
    logging.info("Producer finished streaming all lap events.")


if __name__ == "__main__":
    main()
