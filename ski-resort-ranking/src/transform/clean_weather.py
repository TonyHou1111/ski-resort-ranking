import json
from pathlib import Path

import pandas as pd


def get_latest_raw_file():
    project_root = Path(__file__).resolve().parents[2]
    raw_dir = project_root / "data" / "raw"

    json_files = list(raw_dir.glob("weather_*.json"))
    if not json_files:
        raise FileNotFoundError("No raw weather files found in data/raw")

    latest_file = max(json_files, key=lambda p: p.stat().st_mtime)
    return latest_file


def load_raw_data(file_path: Path):
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def transform_one_resort(raw: dict) -> pd.DataFrame:
    resort_name = raw.get("resort_name")
    fetched_at = raw.get("fetched_at")

    hourly = raw.get("raw_data", {}).get("hourly", {})
    times = hourly.get("time", [])
    snowfall = hourly.get("snowfall", [])
    temperatures = hourly.get("temperature_2m", [])

    min_len = min(len(times), len(snowfall), len(temperatures))

    df = pd.DataFrame({
        "resort_name": [resort_name] * min_len,
        "fetched_at": [fetched_at] * min_len,
        "time": times[:min_len],
        "snowfall": snowfall[:min_len],
        "temperature_2m": temperatures[:min_len],
    })

    return df


def transform_weather_data(raw_data) -> pd.DataFrame:
    all_dfs = [transform_one_resort(raw) for raw in raw_data]
    df = pd.concat(all_dfs, ignore_index=True)

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["fetched_at"] = pd.to_datetime(df["fetched_at"], errors="coerce")
    df["snowfall"] = pd.to_numeric(df["snowfall"], errors="coerce")
    df["temperature_2m"] = pd.to_numeric(df["temperature_2m"], errors="coerce")

    return df


def save_processed_data(
    df: pd.DataFrame,
    output_path: Path | None = None,
) -> Path:
    project_root = Path(__file__).resolve().parents[2]
    processed_dir = project_root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_path or (processed_dir / "weather_cleaned.csv")
    df.to_csv(output_file, index=False)

    print(f"Saved cleaned data to: {output_file}")
    return output_file


if __name__ == "__main__":
    latest_file = get_latest_raw_file()
    print(f"Using raw file: {latest_file}")

    raw_data = load_raw_data(latest_file)
    df = transform_weather_data(raw_data)

    print(df.head())
    print(df["resort_name"].value_counts())
    print(df.info())

    save_processed_data(df)
