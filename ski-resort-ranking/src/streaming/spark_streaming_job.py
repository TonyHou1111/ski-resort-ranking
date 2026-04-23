import argparse
import os
import shutil
import subprocess
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
DEFAULT_JAVA_HOME_CANDIDATES = [
    Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
    Path("/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"),
]


def ensure_java_home() -> None:
    java_home = os.environ.get("JAVA_HOME")
    if java_home and _java_command_works(Path(java_home) / "bin" / "java"):
        return

    java_path = shutil.which("java")
    if java_path and _java_command_works(Path(java_path)):
        return

    for candidate in DEFAULT_JAVA_HOME_CANDIDATES:
        if _java_command_works(candidate / "bin" / "java"):
            os.environ["JAVA_HOME"] = str(candidate)
            os.environ["PATH"] = f"{candidate / 'bin'}:{os.environ.get('PATH', '')}"
            print(f"Using JAVA_HOME={candidate}")
            return

    raise RuntimeError(
        "Java runtime not found. Install OpenJDK 17/21 and set JAVA_HOME or add java to PATH."
    )


def _java_command_works(java_path: Path) -> bool:
    if not java_path.exists():
        return False

    result = subprocess.run(
        [str(java_path), "-version"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


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
        StructField("wind_speed", DoubleType(), True),
        StructField("snow_depth", DoubleType(), True),
        StructField("snow_score", DoubleType(), True),
        StructField("temp_score", DoubleType(), True),
        StructField("wind_score", DoubleType(), True),
        StructField("depth_score", DoubleType(), True),
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


def write_parquet_snapshot(batch_df, batch_id: int, output_dir: str) -> None:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    enriched_batch = (
        batch_df
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .withColumn("snapshot_batch_id", F.lit(batch_id))
        .withColumn("snapshot_written_at", F.current_timestamp())
    )

    (
        enriched_batch
        .write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"Wrote parquet snapshot for batch {batch_id} to {output_path}")


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
        default="parquet",
        help="Where to write aggregated output. Default: parquet",
    )
    parser.add_argument(
        "--stream-output-mode",
        choices=["append", "update", "complete"],
        default="complete",
        help="Structured Streaming output mode. Default: complete",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "data" / "out" / "streaming_summary"),
        help="Directory for parquet snapshot output when output mode is parquet.",
    )
    parser.add_argument(
        "--trigger-seconds",
        type=int,
        default=10,
        help="Micro-batch trigger interval in seconds. Default: 10",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    ensure_java_home()
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
    Path(args.checkpoint_dir).mkdir(parents=True, exist_ok=True)
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    writer = (
        summary_df.writeStream
        .outputMode(args.stream_output_mode)
        .option("checkpointLocation", args.checkpoint_dir)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
    )

    if args.output_mode == "parquet":
        query = (
            writer
            .foreachBatch(
                lambda batch_df, batch_id: write_parquet_snapshot(
                    batch_df=batch_df,
                    batch_id=batch_id,
                    output_dir=args.output_dir,
                )
            )
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
