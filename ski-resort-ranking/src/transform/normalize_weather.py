from pathlib import Path

import pandas as pd


def make_resort_id(resort_name: str) -> str:
    return resort_name.lower().replace(" ", "_")


def get_numeric_column(df: pd.DataFrame, column_name: str) -> pd.Series:
    if column_name not in df.columns:
        return pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[column_name], errors="coerce")


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = pd.DataFrame()

    normalized_df["resort_id"] = df["resort_name"].apply(make_resort_id)
    normalized_df["resort_name"] = df["resort_name"]
    normalized_df["fetched_at"] = pd.to_datetime(df["fetched_at"])
    normalized_df["event_time"] = pd.to_datetime(df["time"])
    normalized_df["snowfall"] = get_numeric_column(df, "snowfall")
    normalized_df["temperature"] = get_numeric_column(df, "temperature_2m")
    normalized_df["wind_speed"] = get_numeric_column(df, "wind_speed_10m")
    normalized_df["snow_depth"] = get_numeric_column(df, "snow_depth")

    return normalized_df


def save_normalized_data(
    df: pd.DataFrame,
    output_path: Path | None = None,
) -> Path:
    project_root = Path(__file__).resolve().parents[2]
    processed_dir = project_root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_path or (processed_dir / "weather_normalized.csv")
    df.to_csv(output_file, index=False)

    print(f"\nSaved normalized data to: {output_file}")
    return output_file


def main():
    project_root = Path(__file__).resolve().parents[2]
    input_file = project_root / "data" / "processed" / "weather_cleaned.csv"
    output_file = project_root / "data" / "processed" / "weather_normalized.csv"

    df = pd.read_csv(input_file)
    normalized_df = normalize_dataframe(df)

    print("Normalized data preview:")
    print(normalized_df.head())

    print("\nUnique resorts:")
    print(normalized_df["resort_name"].unique())

    print("\nRow count by resort:")
    print(normalized_df["resort_name"].value_counts())

    save_normalized_data(normalized_df, output_file)


if __name__ == "__main__":
    main()
