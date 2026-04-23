import argparse
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.fetch.fetch_weather import fetch_weather_data, save_raw_data
from src.ranking.compute_score import compute_scores, save_scored_data
from src.storage.file_sink import write_jsonl_batch
from src.storage.kafka_producer import create_producer, dataframe_to_messages, publish_messages
from src.transform.clean_weather import save_processed_data, transform_weather_data
from src.transform.normalize_weather import normalize_dataframe, save_normalized_data


def run_batch(
    save_intermediate: bool = True,
    output_mode: str = "kafka",
    landing_dir: Path | None = None,
    bootstrap_servers: str | None = None,
) -> int:
    raw_data = fetch_weather_data()

    if save_intermediate:
        save_raw_data(raw_data)

    cleaned_df = transform_weather_data(raw_data)
    normalized_df = normalize_dataframe(cleaned_df)
    scored_df = compute_scores(normalized_df)

    if save_intermediate:
        save_processed_data(cleaned_df)
        save_normalized_data(normalized_df)
        save_scored_data(scored_df)

    messages = dataframe_to_messages(scored_df)
    if not messages:
        print("No messages generated for this batch.")
        return 0

    if output_mode == "file":
        target_dir = landing_dir or (PROJECT_ROOT / "data" / "landing")
        write_jsonl_batch(messages, target_dir)
    else:
        producer = create_producer(bootstrap_servers=bootstrap_servers)
        try:
            publish_messages(messages, producer=producer)
        finally:
            producer.close()

    print(f"Batch complete. Produced {len(messages)} messages.")
    return len(messages)


def run_loop(
    interval_minutes: float,
    save_intermediate: bool = True,
    output_mode: str = "kafka",
    landing_dir: Path | None = None,
    bootstrap_servers: str | None = None,
) -> None:
    interval_seconds = max(interval_minutes, 0.1) * 60

    while True:
        print("\nStarting streaming batch...")
        run_batch(
            save_intermediate=save_intermediate,
            output_mode=output_mode,
            landing_dir=landing_dir,
            bootstrap_servers=bootstrap_servers,
        )
        print(f"Sleeping for {interval_minutes} minute(s)...")
        time.sleep(interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch ski weather data on a schedule and publish scored records to Kafka.",
    )
    parser.add_argument(
        "--interval-minutes",
        type=float,
        default=5.0,
        help="Minutes to wait between batches. Default: 5",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run a single batch instead of looping.",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Skip saving raw and processed snapshots.",
    )
    parser.add_argument(
        "--output-mode",
        choices=["kafka", "file"],
        default="kafka",
        help="Where to send scored records. Default: kafka",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        help="Kafka bootstrap servers. Defaults to KAFKA_BOOTSTRAP_SERVERS or localhost:9092.",
    )
    parser.add_argument(
        "--landing-dir",
        default=str(PROJECT_ROOT / "data" / "landing"),
        help="Directory for file-stream batches when output mode is file.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    save_intermediate = not args.no_save
    landing_dir = Path(args.landing_dir)

    if args.run_once:
        run_batch(
            save_intermediate=save_intermediate,
            output_mode=args.output_mode,
            landing_dir=landing_dir,
            bootstrap_servers=args.bootstrap_servers,
        )
    else:
        run_loop(
            interval_minutes=args.interval_minutes,
            save_intermediate=save_intermediate,
            output_mode=args.output_mode,
            landing_dir=landing_dir,
            bootstrap_servers=args.bootstrap_servers,
        )


if __name__ == "__main__":
    main()
