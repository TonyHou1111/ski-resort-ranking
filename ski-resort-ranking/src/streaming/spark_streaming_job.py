import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


def build_spark_session(source: str) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("SkiResortStreaming")
        .config("spark.sql.shuffle.partitions", "2")
    )

    spark_master_port = os.environ.get("SPARK_MASTER_PORT")
    slurmd_nodename = os.environ.get("SLURMD_NODENAME")
    if spark_master_port and slurmd_nodename:
        builder = builder.master(f"spark://{slurmd_nodename}:{spark_master_port}")
    else:
        builder = builder.master("local[*]")

    if source == "kafka":
        builder = builder.config("spark.jars.packages", DEFAULT_KAFKA_PACKAGE)

    return builder.getOrCreate()


def get_schema() -> StructType:
    return StructType([
        StructField("resort_id", StringType(), True),
        StructField("resort_name", StringType(), True),
        StructField("fetched_at", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("snowfall", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("snow_score", DoubleType(), True),
        StructField("temp_score", DoubleType(), True),
        StructField("overall_score", DoubleType(), True),
        StructField("ingest_time", StringType(), True),
        StructField("batch_id", StringType(), True),
    ])


def build_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str):
    schema = get_schema()

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) AS payload")
        .select(F.from_json("payload", schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingest_time", F.to_timestamp("ingest_time"))
        .withWatermark("ingest_time", "30 minutes")
    )


def build_file_stream(spark: SparkSession, input_dir: str):
    return (
        spark.readStream
        .schema(get_schema())
        .json(input_dir)
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingest_time", F.to_timestamp("ingest_time"))
        .withWatermark("ingest_time", "30 minutes")
    )


def build_summary(stream_df):
    return (
        stream_df
        .groupBy(
            F.window("ingest_time", "10 minutes"),
            F.col("resort_name"),
        )
        .agg(
            F.avg("overall_score").alias("avg_score"),
            F.sum("snowfall").alias("total_snowfall"),
            F.avg("temperature").alias("avg_temperature"),
            F.count("*").alias("records"),
        )
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Spark Structured Streaming on Kafka or file-based ski weather batches.",
    )
    parser.add_argument(
        "--source",
        choices=["kafka", "file"],
        default="kafka",
        help="Streaming input source. Default: kafka",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers for kafka source.",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "ski_weather_scored"),
        help="Kafka topic for kafka source.",
    )
    parser.add_argument(
        "--input-dir",
        default=str(PROJECT_ROOT / "data" / "landing"),
        help="Directory containing JSON batch files for file source.",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default=str(PROJECT_ROOT / "tmp" / "checkpoint_streaming"),
        help="Checkpoint directory for Spark Structured Streaming.",
    )
    parser.add_argument(
        "--output-mode",
        choices=["console", "parquet"],
        default="console",
        help="Where to write aggregated output. Default: console",
    )
    parser.add_argument(
        "--stream-output-mode",
        choices=["append", "update", "complete"],
        default="complete",
        help="Structured Streaming output mode. Default: complete",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "data" / "out"),
        help="Directory for parquet output when output mode is parquet.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark_session(args.source)
    spark.sparkContext.setLogLevel("WARN")

    if args.source == "kafka":
        stream_df = build_kafka_stream(
            spark=spark,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
        )
    else:
        stream_df = build_file_stream(spark, args.input_dir)

    summary_df = build_summary(stream_df)
    writer = (
        summary_df.writeStream
        .outputMode(args.stream_output_mode)
        .option("checkpointLocation", args.checkpoint_dir)
    )

    if args.output_mode == "parquet":
        query = (
            writer
            .format("parquet")
            .option("path", args.output_dir)
            .start()
        )
    else:
        query = (
            writer
            .format("console")
            .option("truncate", False)
            .start()
        )

    query.awaitTermination()


if __name__ == "__main__":
    main()
