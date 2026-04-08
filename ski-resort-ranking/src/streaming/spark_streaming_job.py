import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def build_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("SkiResortFileStreaming")
        .config("spark.sql.shuffle.partitions", "2")
    )

    spark_master_port = os.environ.get("SPARK_MASTER_PORT")
    slurmd_nodename = os.environ.get("SLURMD_NODENAME")
    if spark_master_port and slurmd_nodename:
        builder = builder.master(f"spark://{slurmd_nodename}:{spark_master_port}")
    else:
        builder = builder.master("local[*]")

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


def build_stream(spark: SparkSession, input_dir: str):
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
        description="Read scored weather batches from a landing folder with Spark Structured Streaming.",
    )
    parser.add_argument(
        "--input-dir",
        default=str(PROJECT_ROOT / "data" / "landing"),
        help="Directory containing JSON batch files.",
    )
    parser.add_argument(
        "--checkpoint-dir",
        default=str(PROJECT_ROOT / "tmp" / "checkpoint_file_stream"),
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
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    stream_df = build_stream(spark, args.input_dir)
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
