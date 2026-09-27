"""
Prometheus-Compatible Metrics Collection — Phase II
Tracks pipeline latency, throughput, error rates, and Kafka consumer lag.
Exposes /metrics endpoint for Prometheus scraping.
"""
import time
import threading
import logging
from collections import defaultdict
from datetime import datetime, timezone

try:
    from prometheus_client import (
        Counter, Histogram, Gauge, Info,
        generate_latest, CONTENT_TYPE_LATEST,
    )
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# Prometheus Metrics Definitions
# ══════════════════════════════════════════════════════════════

if HAS_PROMETHEUS:
    # ── Counters ─────────────────────────────────────────────
    RECORDS_PROCESSED = Counter(
        "f1_pipeline_records_processed_total",
        "Total records processed by the pipeline",
        ["component", "status"],  # status: valid, invalid, error
    )

    RECORDS_WRITTEN_ICEBERG = Counter(
        "f1_pipeline_iceberg_writes_total",
        "Total records written to Iceberg tables",
        ["table_name"],
    )

    ALERTS_SENT = Counter(
        "f1_pipeline_alerts_sent_total",
        "Total alerts dispatched",
        ["alert_type", "severity"],
    )

    # ── Histograms ───────────────────────────────────────────
    PROCESSING_LATENCY = Histogram(
        "f1_pipeline_processing_latency_seconds",
        "End-to-end processing latency (ingestion to write)",
        ["component"],
        buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
    )

    BATCH_DURATION = Histogram(
        "f1_pipeline_batch_duration_seconds",
        "Duration of each micro-batch write",
        ["table_name"],
        buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
    )

    # ── Gauges ───────────────────────────────────────────────
    KAFKA_CONSUMER_LAG = Gauge(
        "f1_pipeline_kafka_consumer_lag",
        "Current Kafka consumer lag (messages behind)",
        ["topic", "partition"],
    )

    ICEBERG_SNAPSHOT_COUNT = Gauge(
        "f1_pipeline_iceberg_snapshots",
        "Number of Iceberg snapshots for a table",
        ["table_name"],
    )

    ICEBERG_DATA_FILES = Gauge(
        "f1_pipeline_iceberg_data_files",
        "Number of data files in an Iceberg table",
        ["table_name"],
    )

    PIPELINE_UP = Gauge(
        "f1_pipeline_up",
        "Whether the pipeline component is running (1=up, 0=down)",
        ["component"],
    )

    # ── Info ─────────────────────────────────────────────────
    PIPELINE_INFO = Info(
        "f1_pipeline",
        "Pipeline metadata",
    )


# ══════════════════════════════════════════════════════════════
# Convenience Functions (work even without prometheus_client)
# ══════════════════════════════════════════════════════════════

class MetricsCollector:
    """
    Lightweight metrics collector that works with or without Prometheus.
    Stores recent metrics in memory for API access.
    """

    def __init__(self):
        self._metrics = defaultdict(list)
        self._lock = threading.Lock()
        self._max_history = 1000

    def record_processed(self, component: str, status: str, count: int = 1):
        """Record that records were processed."""
        if HAS_PROMETHEUS:
            RECORDS_PROCESSED.labels(component=component, status=status).inc(count)
        with self._lock:
            self._metrics["records_processed"].append({
                "component": component, "status": status,
                "count": count, "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            self._trim("records_processed")

    def record_latency(self, component: str, latency_seconds: float):
        """Record processing latency."""
        if HAS_PROMETHEUS:
            PROCESSING_LATENCY.labels(component=component).observe(latency_seconds)
        with self._lock:
            self._metrics["latency"].append({
                "component": component, "latency_s": round(latency_seconds, 4),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            self._trim("latency")

    def record_iceberg_write(self, table_name: str, count: int = 1):
        """Record Iceberg table write."""
        if HAS_PROMETHEUS:
            RECORDS_WRITTEN_ICEBERG.labels(table_name=table_name).inc(count)

    def record_batch_duration(self, table_name: str, duration_seconds: float):
        """Record micro-batch write duration."""
        if HAS_PROMETHEUS:
            BATCH_DURATION.labels(table_name=table_name).observe(duration_seconds)

    def set_consumer_lag(self, topic: str, partition: int, lag: int):
        """Set current Kafka consumer lag."""
        if HAS_PROMETHEUS:
            KAFKA_CONSUMER_LAG.labels(topic=topic, partition=str(partition)).set(lag)

    def set_pipeline_up(self, component: str, is_up: bool = True):
        """Set pipeline component status."""
        if HAS_PROMETHEUS:
            PIPELINE_UP.labels(component=component).set(1 if is_up else 0)

    def record_alert(self, alert_type: str, severity: str):
        """Record that an alert was sent."""
        if HAS_PROMETHEUS:
            ALERTS_SENT.labels(alert_type=alert_type, severity=severity).inc()

    def get_recent_metrics(self, metric_name: str = None, limit: int = 100) -> dict:
        """Get recent in-memory metrics for API access."""
        with self._lock:
            if metric_name:
                return {metric_name: self._metrics.get(metric_name, [])[-limit:]}
            return {k: v[-limit:] for k, v in self._metrics.items()}

    def _trim(self, key: str):
        """Trim metric history to max size."""
        if len(self._metrics[key]) > self._max_history:
            self._metrics[key] = self._metrics[key][-self._max_history:]


# ── Module-level singleton ───────────────────────────────────
collector = MetricsCollector()


def get_prometheus_metrics() -> bytes:
    """Generate Prometheus-format metrics output."""
    if HAS_PROMETHEUS:
        return generate_latest()
    return b"# prometheus_client not installed\n"


def get_prometheus_content_type() -> str:
    """Get the Prometheus content type header."""
    if HAS_PROMETHEUS:
        return CONTENT_TYPE_LATEST
    return "text/plain"
