from pathlib import Path

import pandas as pd


def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # More snowfall is better.
    df["snow_score"] = df["snowfall"]

    # Temperature closer to 0C is better.
    df["temp_score"] = -abs(df["temperature"] - 0)

    df["overall_score"] = df["snow_score"] + df["temp_score"]

    return df


def save_scored_data(
    df: pd.DataFrame,
    output_path: Path | None = None,
) -> Path:
    project_root = Path(__file__).resolve().parents[2]
    processed_dir = project_root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_path or (processed_dir / "weather_scored.csv")
    df.to_csv(output_file, index=False)

    print(f"\nSaved scored data to: {output_file}")
    return output_file


def main():
    project_root = Path(__file__).resolve().parents[2]
    input_file = project_root / "data" / "processed" / "weather_normalized.csv"
    output_file = project_root / "data" / "processed" / "weather_scored.csv"

    df = pd.read_csv(input_file)
    df["event_time"] = pd.to_datetime(df["event_time"])

    scored_df = compute_scores(df)

    print("Score preview:")
    print(scored_df.head())

    save_scored_data(scored_df, output_file)


if __name__ == "__main__":
    main()
