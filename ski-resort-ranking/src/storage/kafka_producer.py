import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


TOPIC_NAME = "ski_weather_scored"
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"


def get_bootstrap_servers() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)


def create_producer(bootstrap_servers: str | None = None) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers or get_bootstrap_servers(),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def load_scored_data() -> pd.DataFrame:
    project_root = Path(__file__).resolve().parents[2]
    input_file = project_root / "data" / "processed" / "weather_scored.csv"

    if not input_file.exists():
        raise FileNotFoundError(f"Scored file not found: {input_file}")

    return pd.read_csv(input_file)


def _serialize_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def dataframe_to_messages(df: pd.DataFrame) -> list[dict]:
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ingest_time = datetime.now(timezone.utc).isoformat()
    messages = []

    for row in df.to_dict(orient="records"):
        message = {key: _serialize_value(value) for key, value in row.items()}
        message["ingest_time"] = ingest_time
        message["batch_id"] = batch_id
        messages.append(message)

    return messages


def publish_messages(
    messages: list[dict],
    producer: KafkaProducer | None = None,
):
    owns_producer = producer is None
    producer = producer or create_producer()

    for message in messages:
        producer.send(TOPIC_NAME, key=message["resort_id"], value=message)

    producer.flush()
    if owns_producer:
        producer.close()

    print(
        f"Published {len(messages)} messages to topic '{TOPIC_NAME}' "
        f"on {get_bootstrap_servers()}"
    )


def main():
    df = load_scored_data()
    messages = dataframe_to_messages(df)

    if messages:
        print("Sample message:")
        print(messages[0])

    publish_messages(messages)


if __name__ == "__main__":
    main()
