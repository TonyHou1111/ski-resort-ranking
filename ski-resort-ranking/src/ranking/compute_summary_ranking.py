from pathlib import Path
import pandas as pd


def load_scored_data():
    project_root = Path(__file__).resolve().parents[2]
    input_file = project_root / "data" / "processed" / "weather_scored.csv"

    if not input_file.exists():
        raise FileNotFoundError(f"Scored file not found: {input_file}")

    return pd.read_csv(input_file)


def compute_summary_ranking(df: pd.DataFrame) -> pd.DataFrame:
    summary_df = (
        df.groupby("resort_name", as_index=False)
        .agg(
            total_snowfall=("snowfall", "sum"),
            avg_temperature=("temperature", "mean"),
            avg_score=("overall_score", "mean"),
            records=("event_time", "count")
        )
    )

    # Rank resorts by their average score across the available forecast horizon.
    summary_df = summary_df.sort_values(by="avg_score", ascending=False).reset_index(drop=True)
    summary_df["rank"] = summary_df.index + 1

    summary_df = summary_df[
        ["rank", "resort_name", "avg_score", "total_snowfall", "avg_temperature", "records"]
    ]

    return summary_df


def save_summary_ranking(summary_df: pd.DataFrame):
    project_root = Path(__file__).resolve().parents[2]
    final_dir = project_root / "data" / "final"
    final_dir.mkdir(parents=True, exist_ok=True)

    output_file = final_dir / "summary_ranking.csv"
    summary_df.to_csv(output_file, index=False)

    print(f"Saved summary ranking to: {output_file}")


if __name__ == "__main__":
    df = load_scored_data()
    summary_df = compute_summary_ranking(df)

    print(summary_df)
    save_summary_ranking(summary_df)
