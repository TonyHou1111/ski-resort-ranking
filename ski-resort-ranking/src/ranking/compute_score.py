from pathlib import Path

import pandas as pd


IDEAL_TEMPERATURE_C = -3.0
TEMPERATURE_TOLERANCE_C = 15.0
SNOWFALL_REFERENCE_CM = 20.0
WIND_SPEED_REFERENCE_KMH = 40.0
SNOW_DEPTH_REFERENCE_M = 2.0

WEIGHTS = {
    "snow_score": 0.35,
    "temp_score": 0.25,
    "wind_score": 0.20,
    "depth_score": 0.20,
}


def get_numeric_column(df: pd.DataFrame, column_name: str) -> pd.Series:
    if column_name not in df.columns:
        return pd.Series([0.0] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[column_name], errors="coerce").fillna(0.0)


def clip_score(series: pd.Series) -> pd.Series:
    return series.clip(lower=0.0, upper=1.0) * 100.0


def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    snowfall = get_numeric_column(df, "snowfall")
    temperature = get_numeric_column(df, "temperature")
    wind_speed = get_numeric_column(df, "wind_speed")
    snow_depth = get_numeric_column(df, "snow_depth")

    # Reference values turn raw weather units into bounded 0-100 sub-scores.
    # Higher snowfall and deeper base snow improve ski conditions.
    df["snow_score"] = clip_score(snowfall / SNOWFALL_REFERENCE_CM)
    df["depth_score"] = clip_score(snow_depth / SNOW_DEPTH_REFERENCE_M)

    # Temperature is best near a cold but ski-friendly target range.
    df["temp_score"] = clip_score(
        1.0 - (temperature.sub(IDEAL_TEMPERATURE_C).abs() / TEMPERATURE_TOLERANCE_C)
    )

    # Strong wind reduces comfort and lift reliability.
    df["wind_score"] = clip_score(1.0 - (wind_speed / WIND_SPEED_REFERENCE_KMH))

    weighted_score = sum(df[column] * weight for column, weight in WEIGHTS.items())
    df["overall_score"] = weighted_score.round(2)

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
