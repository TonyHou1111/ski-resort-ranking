from pathlib import Path
import pandas as pd


def compute_ranking(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 在同一个时间点内，按 overall_score 从高到低排名
    df["rank"] = (
        df.groupby("event_time")["overall_score"]
        .rank(method="first", ascending=False)
    )

    # 让结果更直观：先按时间，再按名次排序
    df = df.sort_values(by=["event_time", "rank"]).reset_index(drop=True)

    return df


def main():
    project_root = Path(__file__).resolve().parents[2]
    input_file = project_root / "data" / "processed" / "weather_scored.csv"
    output_file = project_root / "data" / "final" / "ranking.csv"

    if not input_file.exists():
        raise FileNotFoundError(f"Scored file not found: {input_file}")

    df = pd.read_csv(input_file)
    df["event_time"] = pd.to_datetime(df["event_time"])

    ranked_df = compute_ranking(df)

    print("Ranking preview:")
    print(ranked_df.head(10))

    output_file.parent.mkdir(parents=True, exist_ok=True)
    ranked_df.to_csv(output_file, index=False)

    print(f"\nSaved ranking to: {output_file}")


if __name__ == "__main__":
    main()