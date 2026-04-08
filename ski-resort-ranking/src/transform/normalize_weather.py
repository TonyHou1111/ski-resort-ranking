from pathlib import Path
import pandas as pd


def make_resort_id(resort_name: str) -> str:
    return resort_name.lower().replace(" ", "_")


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = pd.DataFrame()

    normalized_df["resort_id"] = df["resort_name"].apply(make_resort_id)
    normalized_df["resort_name"] = df["resort_name"]
    normalized_df["fetched_at"] = pd.to_datetime(df["fetched_at"])
    normalized_df["event_time"] = pd.to_datetime(df["time"])
    normalized_df["snowfall"] = pd.to_numeric(df["snowfall"], errors="coerce")
    normalized_df["temperature"] = pd.to_numeric(df["temperature_2m"], errors="coerce")

    return normalized_df


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

    output_file.parent.mkdir(parents=True, exist_ok=True)
    normalized_df.to_csv(output_file, index=False)

    print(f"\nSaved normalized data to: {output_file}")


if __name__ == "__main__":
    main()