"""
consumers/spark_processor.py
─────────────────────────────────────────────────────────────────
Spark Structured Streaming application that:
  1. Consumes all 5 Kafka topics simultaneously
  2. Infers/applies explicit schemas to JSON payloads
  3. Writes Parquet output partitioned by (date, event_type) to:
       • AWS S3  (s3a://<bucket>/smart-city/<topic>/)
       • Local   (file:///output/smart-city/<topic>/) if S3 not set

Run inside Docker (submitted to Spark cluster):
    docker exec spark-master spark-submit \\
        --master spark://spark-master:7077 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\\
                   org.apache.hadoop:hadoop-aws:3.3.4,\\
                   com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
        /opt/bitnami/spark/jobs/spark_processor.py

Run locally (for testing):
    spark-submit \\
        --master local[*] \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \\
        consumers/spark_processor.py
"""

import os
import sys
from pathlib import Path

# Allow importing from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, LongType, FloatType,
)

# ── Configuration ──────────────────────────────────────────────
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
S3_BUCKET     = os.getenv("S3_BUCKET_NAME", "")
AWS_KEY       = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET    = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION    = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

USE_S3 = bool(S3_BUCKET and AWS_KEY and AWS_SECRET)

BASE_OUTPUT = (
    f"s3a://{S3_BUCKET}/smart-city" if USE_S3
    else str(Path(__file__).resolve().parent.parent / "output" / "smart-city")
)
CHECKPOINT_BASE = (
    f"s3a://{S3_BUCKET}/checkpoints" if USE_S3
    else str(Path(__file__).resolve().parent.parent / "output" / "checkpoints")
)

TOPICS = {
    "vehicle_data":   "VEHICLE",
    "gps_data":       "GPS",
    "weather_data":   "WEATHER",
    "telemetry_data": "TELEMETRY",
    "emergency_data": "TEST_FAILURE",
}

# ── Schemas ────────────────────────────────────────────────────

GPS_SCHEMA = StructType([
    StructField("vehicle_id",      StringType(),  True),
    StructField("event_type",      StringType(),  True),
    StructField("timestamp",       StringType(),  True),
    StructField("lat",             DoubleType(),  True),
    StructField("lon",             DoubleType(),  True),
    StructField("elevation_m",     FloatType(),   True),
    StructField("speed_mps",       FloatType(),   True),
    StructField("speed_kmh",       FloatType(),   True),
    StructField("heading_deg",     FloatType(),   True),
    StructField("gpx_timestamp",   StringType(),  True),
])

WEATHER_SCHEMA = StructType([
    StructField("vehicle_id",         StringType(),  True),
    StructField("event_type",         StringType(),  True),
    StructField("timestamp",          StringType(),  True),
    StructField("lat",                DoubleType(),  True),
    StructField("lon",                DoubleType(),  True),
    StructField("temperature_c",      FloatType(),   True),
    StructField("feels_like_c",       FloatType(),   True),
    StructField("humidity_pct",       IntegerType(), True),
    StructField("pressure_hpa",       IntegerType(), True),
    StructField("wind_speed_mps",     FloatType(),   True),
    StructField("wind_direction_deg", IntegerType(), True),
    StructField("wind_gust_mps",      FloatType(),   True),
    StructField("visibility_m",       IntegerType(), True),
    StructField("description",        StringType(),  True),
    StructField("cloud_coverage_pct", IntegerType(), True),
    StructField("rain_1h_mm",         FloatType(),   True),
    StructField("snow_1h_mm",         FloatType(),   True),
    StructField("aqi",                IntegerType(), True),
    StructField("co_ugm3",            FloatType(),   True),
    StructField("no2_ugm3",           FloatType(),   True),
    StructField("o3_ugm3",            FloatType(),   True),
    StructField("pm2_5_ugm3",         FloatType(),   True),
    StructField("pm10_ugm3",          FloatType(),   True),
    StructField("so2_ugm3",           FloatType(),   True),
])

TELEMETRY_SCHEMA = StructType([
    StructField("vehicle_id",        StringType(),  True),
    StructField("event_type",        StringType(),  True),
    StructField("timestamp",         StringType(),  True),
    StructField("lat",               DoubleType(),  True),
    StructField("lon",               DoubleType(),  True),
    StructField("speed_kmh",         FloatType(),   True),
    StructField("rpm",               IntegerType(), True),
    StructField("throttle_pct",      FloatType(),   True),
    StructField("fuel_level_pct",    FloatType(),   True),
    StructField("engine_temp_c",     FloatType(),   True),
    StructField("oil_pressure_psi",  FloatType(),   True),
    StructField("battery_voltage_v", FloatType(),   True),
    StructField("gear",              IntegerType(), True),
    StructField("odometer_km",       DoubleType(),  True),
    StructField("trip_km",           DoubleType(),  True),
])

VEHICLE_SCHEMA = StructType([
    StructField("vehicle_id",        StringType(),  True),
    StructField("event_type",        StringType(),  True),
    StructField("timestamp",         StringType(),  True),
    StructField("lat",               DoubleType(),  True),
    StructField("lon",               DoubleType(),  True),
    StructField("make",              StringType(),  True),
    StructField("model",             StringType(),  True),
    StructField("year",              IntegerType(), True),
    StructField("engine_cc",         IntegerType(), True),
    StructField("speed_kmh",         FloatType(),   True),
    StructField("rpm",               IntegerType(), True),
    StructField("fuel_level_pct",    FloatType(),   True),
    StructField("engine_temp_c",     FloatType(),   True),
    StructField("odometer_km",       DoubleType(),  True),
    StructField("trip_km",           DoubleType(),  True),
])

EMERGENCY_SCHEMA = StructType([
    StructField("vehicle_id",              StringType(),  True),
    StructField("event_type",              StringType(),  True),
    StructField("timestamp",               StringType(),  True),
    StructField("lat",                     DoubleType(),  True),
    StructField("lon",                     DoubleType(),  True),
    StructField("distance_to_office_km",   FloatType(),   True),
    StructField("failure_code",            StringType(),  True),
    StructField("severity",                StringType(),  True),
    StructField("speed_at_event_kmh",      FloatType(),   True),
    StructField("cycle",                   IntegerType(), True),
    StructField("description",             StringType(),  True),
])

TOPIC_SCHEMAS = {
    "vehicle_data":   VEHICLE_SCHEMA,
    "gps_data":       GPS_SCHEMA,
    "weather_data":   WEATHER_SCHEMA,
    "telemetry_data": TELEMETRY_SCHEMA,
    "emergency_data": EMERGENCY_SCHEMA,
}


# ── Spark Session Factory ──────────────────────────────────────

def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("SmartCity-Structured-Streaming")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        # Kafka integration
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ]),
        )
    )

    if USE_S3:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key",          AWS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key",          AWS_SECRET)
            .config("spark.hadoop.fs.s3a.endpoint",            f"s3.{AWS_REGION}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.path.style.access",   "false")
        )
        print(f"[Spark] Writing to S3: s3a://{S3_BUCKET}/smart-city/")
    else:
        print(f"[Spark] ⚠️  S3 not configured — writing locally to: {BASE_OUTPUT}")

    return builder.getOrCreate()


# ── Stream builder ─────────────────────────────────────────────

def build_stream(spark: SparkSession, topic: str, schema: StructType):
    """Read from Kafka, parse JSON, add partition columns, return DataFrame."""
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", f"spark-smart-city-{topic}")
        .load()
    )

    parsed_df = (
        raw_df
        .select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

    # Add partition columns
    enriched_df = (
        parsed_df
        .withColumn("date",       F.to_date(F.col("timestamp")))
        .withColumn("event_type", F.coalesce(F.col("event_type"), F.lit(TOPICS[topic])))
        .withColumn("ingest_ts",  F.current_timestamp())
    )

    return enriched_df


# ── Write stream ───────────────────────────────────────────────

def write_stream(df, topic: str):
    """Write parsed stream to Parquet, partitioned by date + event_type."""
    output_path     = f"{BASE_OUTPUT}/{topic}"
    checkpoint_path = f"{CHECKPOINT_BASE}/{topic}"

    query = (
        df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path",              output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("date", "event_type")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print(f"[Spark] ✅ Stream started: {topic} → {output_path}")
    return query


# ── Entry point ────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    queries = []
    for topic, schema in TOPIC_SCHEMAS.items():
        df    = build_stream(spark, topic, schema)
        query = write_stream(df, topic)
        queries.append(query)

    print(f"\n[Spark] 🚀 All {len(queries)} streams running. Awaiting termination...\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[Spark] Stopping all streams...")
        for q in queries:
            q.stop()
        spark.stop()
        print("[Spark] Shutdown complete.")


if __name__ == "__main__":
    main()
