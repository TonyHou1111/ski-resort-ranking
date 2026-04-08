import json
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


TOPIC_NAME = "ski_weather_scored"


def create_producer() -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    return producer


def load_scored_data() -> pd.DataFrame:
    project_root = Path(__file__).resolve().parents[2]
    input_file = project_root / "data" / "processed" / "weather_scored.csv"

    if not input_file.exists():
        raise FileNotFoundError(f"Scored file not found: {input_file}")

    return pd.read_csv(input_file)


def dataframe_to_messages(df: pd.DataFrame) -> list[dict]:
    return df.to_dict(orient="records")


def publish_messages(messages: list[dict]):
    producer = create_producer()

    for message in messages:
        key = message["resort_id"]
        producer.send(TOPIC_NAME, key=key, value=message)

    producer.flush()
    producer.close()

    print(f"Published {len(messages)} messages to topic '{TOPIC_NAME}'")


def main():
    df = load_scored_data()
    messages = dataframe_to_messages(df)

    print("Sample message:")
    print(messages[0])

    publish_messages(messages)


if __name__ == "__main__":
    main()