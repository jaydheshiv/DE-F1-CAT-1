"""
PySpark Structured Streaming — Kafka to Apache Iceberg
Phase II: Real-Time Lakehouse Pipeline

Reads from Kafka topic 'f1-lap-events', validates, transforms,
and writes to Iceberg tables stored in MinIO (S3-compatible).

Tables produced:
  - lakehouse.raw_lap_events      (all events, partitioned by race_id)
  - lakehouse.dead_letter_queue   (invalid events)
  - lakehouse.cleaned_laps        (validated + enriched laps)
  - lakehouse.agg_driver_race_stats (running aggregations)
"""
import os
import sys
import json
import logging
import uuid
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, TimestampType
)

# ── Configuration ────────────────────────────────────────────
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "f1-lap-events")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "s3a://warehouse/checkpoints")
WAREHOUSE_PATH = os.environ.get("WAREHOUSE_PATH", "s3a://warehouse")
ICEBERG_CATALOG = "lakehouse_catalog"
ICEBERG_DB = "lakehouse"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK-ICEBERG] %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# ── Kafka Event Schema ──────────────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("race_id", IntegerType(), True),
    StructField("driver_id", StringType(), True),
    StructField("driver_code", StringType(), True),
    StructField("driver_name", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("lap_time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("event_type", StringType(), True),
])


def create_spark_session() -> SparkSession:
    """Create SparkSession configured for Iceberg + Kafka + S3 (MinIO)."""
    spark = (
        SparkSession.builder
        .appName("F1-Lakehouse-Streaming")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.type",
            "hive"
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.uri",
            "thrift://hive-metastore:9083"
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
            WAREHOUSE_PATH
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config("spark.sql.catalog.{}.s3.endpoint".format(ICEBERG_CATALOG),
                os.environ.get("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.sql.catalog.{}.s3.access-key-id".format(ICEBERG_CATALOG),
                os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.sql.catalog.{}.s3.secret-access-key".format(ICEBERG_CATALOG),
                os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.sql.catalog.{}.s3.path-style-access".format(ICEBERG_CATALOG), "true")
        # Hadoop S3A config for checkpoint
        .config("spark.hadoop.fs.s3a.endpoint",
                os.environ.get("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key",
                os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.defaultCatalog", ICEBERG_CATALOG)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_iceberg_tables(spark: SparkSession):
    """Create Iceberg database and tables if they don't exist."""

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}")

    # Raw events table — partitioned by race_id
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}.raw_lap_events (
            event_id        STRING,
            race_id         INT,
            driver_id       STRING,
            driver_code     STRING,
            driver_name     STRING,
            lap             INT,
            position        INT,
            lap_time        STRING,
            milliseconds    INT,
            event_type      STRING,
            validation_status STRING,
            ingested_at     TIMESTAMP,
            batch_id        STRING
        )
        USING iceberg
        PARTITIONED BY (race_id)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'commit.retry.num-retries' = '4'
        )
    """)

    # Dead letter queue
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}.dead_letter_queue (
            event_id        STRING,
            raw_payload     STRING,
            error_type      STRING,
            error_message   STRING,
            ingested_at     TIMESTAMP,
            batch_id        STRING
        )
        USING iceberg
        TBLPROPERTIES (
            'write.format.default' = 'parquet'
        )
    """)

    # Cleaned laps (validated + enriched)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}.cleaned_laps (
            event_id        STRING,
            race_id         INT,
            driver_id       STRING,
            driver_code     STRING,
            driver_name     STRING,
            lap             INT,
            position        INT,
            lap_time        STRING,
            milliseconds    INT,
            seconds         DOUBLE,
            is_fastest_lap  BOOLEAN,
            ingested_at     TIMESTAMP,
            processed_at    TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (race_id)
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)

    # Aggregated driver stats per race
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.{ICEBERG_DB}.agg_driver_race_stats (
            race_id         INT,
            driver_id       STRING,
            driver_code     STRING,
            driver_name     STRING,
            total_laps      INT,
            best_position   INT,
            worst_position  INT,
            avg_lap_ms      DOUBLE,
            best_lap_ms     INT,
            total_time_ms   BIGINT,
            last_updated    TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (race_id)
        TBLPROPERTIES (
            'write.format.default' = 'parquet'
        )
    """)

    logger.info("Iceberg tables ensured in database '%s'", ICEBERG_DB)


def validate_event(row) -> tuple:
    """
    Validate a single event. Returns (is_valid, error_type, error_message).
    """
    if not row.driver_id or row.driver_id == "":
        return False, "NULL_DRIVER_ID", "driver_id is missing or null"
    if row.milliseconds is not None and row.milliseconds < 0:
        return False, "NEGATIVE_MILLISECONDS", f"lap time is {row.milliseconds}ms"
    if row.milliseconds is not None and row.milliseconds > 300000:
        return False, "OUTLIER_LAP_TIME", f"{row.milliseconds}ms exceeds 5 minutes"
    return True, "", ""


def process_micro_batch(batch_df: DataFrame, batch_id: int):
    """
    Process each micro-batch from Kafka Structured Streaming.
    Called via foreachBatch for exactly-once semantics.
    """
    if batch_df.isEmpty():
        return

    spark = batch_df.sparkSession
    correlation_id = str(uuid.uuid4())[:8]
    now = datetime.utcnow()
    record_count = batch_df.count()

    logger.info(
        "Processing batch %d | %d records | correlation_id=%s",
        batch_id, record_count, correlation_id
    )

    # ── Parse JSON values from Kafka ─────────────────────────
    parsed_df = (
        batch_df
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_id", F.expr("uuid()"))
        .withColumn("ingested_at", F.lit(now).cast(TimestampType()))
        .withColumn("batch_id", F.lit(correlation_id))
    )

    # ── Validate events ──────────────────────────────────────
    # UDF-free validation using SQL expressions
    validated_df = parsed_df.withColumn(
        "is_valid",
        F.when(F.col("driver_id").isNull() | (F.col("driver_id") == ""), False)
        .when(F.col("milliseconds").isNotNull() & (F.col("milliseconds") < 0), False)
        .when(F.col("milliseconds").isNotNull() & (F.col("milliseconds") > 300000), False)
        .otherwise(True)
    ).withColumn(
        "error_type",
        F.when(F.col("driver_id").isNull() | (F.col("driver_id") == ""), "NULL_DRIVER_ID")
        .when(F.col("milliseconds").isNotNull() & (F.col("milliseconds") < 0),
              "NEGATIVE_MILLISECONDS")
        .when(F.col("milliseconds").isNotNull() & (F.col("milliseconds") > 300000),
              "OUTLIER_LAP_TIME")
        .otherwise("")
    ).withColumn(
        "error_message",
        F.when(F.col("error_type") == "NULL_DRIVER_ID", "driver_id is missing or null")
        .when(F.col("error_type") == "NEGATIVE_MILLISECONDS",
              F.concat(F.lit("lap time is "), F.col("milliseconds").cast("string"), F.lit("ms")))
        .when(F.col("error_type") == "OUTLIER_LAP_TIME",
              F.concat(F.col("milliseconds").cast("string"), F.lit("ms exceeds 5 minutes")))
        .otherwise("")
    )

    # ── Split valid vs invalid ───────────────────────────────
    valid_df = validated_df.filter(F.col("is_valid") == True)
    invalid_df = validated_df.filter(F.col("is_valid") == False)

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()

    # ── Write RAW events (all) to raw_lap_events ─────────────
    raw_write_df = validated_df.select(
        "event_id", "race_id", "driver_id", "driver_code", "driver_name",
        "lap", "position", "lap_time", "milliseconds", "event_type",
        F.when(F.col("is_valid"), "VALID").otherwise("INVALID").alias("validation_status"),
        "ingested_at", "batch_id"
    )
    raw_write_df.writeTo(
        f"{ICEBERG_CATALOG}.{ICEBERG_DB}.raw_lap_events"
    ).append()
    logger.info("Wrote %d raw events to raw_lap_events", record_count)

    # ── Write INVALID events to dead_letter_queue ────────────
    if invalid_count > 0:
        dlq_df = invalid_df.select(
            F.col("event_id"),
            F.to_json(F.struct("*")).alias("raw_payload"),
            F.col("error_type"),
            F.col("error_message"),
            F.col("ingested_at"),
            F.col("batch_id"),
        )
        dlq_df.writeTo(
            f"{ICEBERG_CATALOG}.{ICEBERG_DB}.dead_letter_queue"
        ).append()
        logger.warning("Wrote %d invalid events to DLQ", invalid_count)

    # ── Write VALID events to cleaned_laps (enriched) ────────
    if valid_count > 0:
        cleaned_df = valid_df.select(
            "event_id", "race_id", "driver_id", "driver_code", "driver_name",
            "lap", "position", "lap_time", "milliseconds",
            (F.col("milliseconds") / 1000.0).alias("seconds"),
            F.lit(False).alias("is_fastest_lap"),  # Updated by aggregation later
            "ingested_at",
            F.lit(now).cast(TimestampType()).alias("processed_at"),
        )
        cleaned_df.writeTo(
            f"{ICEBERG_CATALOG}.{ICEBERG_DB}.cleaned_laps"
        ).append()
        logger.info("Wrote %d cleaned laps", valid_count)

        # ── Compute aggregated stats per driver per race ─────
        agg_df = (
            valid_df
            .groupBy("race_id", "driver_id", "driver_code", "driver_name")
            .agg(
                F.count("*").alias("total_laps"),
                F.min("position").alias("best_position"),
                F.max("position").alias("worst_position"),
                F.avg("milliseconds").alias("avg_lap_ms"),
                F.min("milliseconds").alias("best_lap_ms"),
                F.sum("milliseconds").cast(LongType()).alias("total_time_ms"),
            )
            .withColumn("last_updated", F.lit(now).cast(TimestampType()))
        )

        # Merge (overwrite partition) for idempotent aggregation
        agg_df.writeTo(
            f"{ICEBERG_CATALOG}.{ICEBERG_DB}.agg_driver_race_stats"
        ).overwritePartitions()
        logger.info("Updated aggregated driver stats")

    logger.info(
        "Batch %d complete | valid=%d invalid=%d | correlation_id=%s",
        batch_id, valid_count, invalid_count, correlation_id
    )


def main():
    """Entry point: start Structured Streaming pipeline."""
    logger.info("Starting F1 Lakehouse Streaming Pipeline...")

    spark = create_spark_session()
    logger.info("SparkSession created with Iceberg catalog")

    # Ensure tables exist
    ensure_iceberg_tables(spark)

    # ── Read from Kafka ──────────────────────────────────────
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 100)
        .load()
    )

    logger.info("Kafka stream reader configured for topic '%s'", KAFKA_TOPIC)

    # ── Write using foreachBatch ─────────────────────────────
    query = (
        kafka_df.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/f1-lakehouse-stream")
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("Streaming query started — awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
