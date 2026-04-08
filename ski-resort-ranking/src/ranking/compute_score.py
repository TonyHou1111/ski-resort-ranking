from pathlib import Path
import pandas as pd


def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # snowfall 越大越好
    df["snow_score"] = df["snowfall"]

    # temperature 越接近 0 越好
    df["temp_score"] = -abs(df["temperature"] - 0)

    # 总分
    df["overall_score"] = df["snow_score"] + df["temp_score"]

    return df


def main():
    project_root = Path(__file__).resolve().parents[2]
    input_file = project_root / "data" / "processed" / "weather_normalized.csv"
    output_file = project_root / "data" / "processed" / "weather_scored.csv"

    df = pd.read_csv(input_file)
    df["event_time"] = pd.to_datetime(df["event_time"])

    scored_df = compute_scores(df)

    print("Score preview:")
    print(scored_df.head())

    scored_df.to_csv(output_file, index=False)

    print(f"\nSaved scored data to: {output_file}")


if __name__ == "__main__":
    main()