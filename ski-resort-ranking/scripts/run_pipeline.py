from pathlib import Path

from src.fetch.fetch_weather import fetch_weather_data, save_raw_data
from src.transform.clean_weather import transform_weather_data, save_processed_data
from src.transform.normalize_weather import normalize_dataframe
from src.ranking.compute_score import compute_scores
from src.ranking.compute_ranking import compute_ranking
from src.ranking.compute_summary_ranking import compute_summary_ranking


def run_pipeline():
    print("Step 1: Fetch weather data...")
    raw_data = fetch_weather_data()
    save_raw_data(raw_data)

    print("Step 2: Clean data...")
    cleaned_df = transform_weather_data(raw_data)
    save_processed_data(cleaned_df)

    print("Step 3: Normalize data...")
    normalized_df = normalize_dataframe(cleaned_df)

    project_root = Path(__file__).resolve().parents[1]
    processed_dir = project_root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    normalized_file = processed_dir / "weather_normalized.csv"
    normalized_df.to_csv(normalized_file, index=False)
    print(f"Saved normalized data to: {normalized_file}")

    print("Step 4: Compute scores...")
    scored_df = compute_scores(normalized_df)

    scored_file = processed_dir / "weather_scored.csv"
    scored_df.to_csv(scored_file, index=False)
    print(f"Saved scored data to: {scored_file}")

    print("Step 5: Compute event-time ranking...")
    ranked_df = compute_ranking(scored_df)

    final_dir = project_root / "data" / "final"
    final_dir.mkdir(parents=True, exist_ok=True)

    ranking_file = final_dir / "pipeline_ranking.csv"
    ranked_df.to_csv(ranking_file, index=False)
    print(f"Saved event-time ranking to: {ranking_file}")

    print("Step 6: Compute summary ranking...")
    summary_df = compute_summary_ranking(scored_df)

    summary_file = final_dir / "summary_ranking.csv"
    summary_df.to_csv(summary_file, index=False)
    print(f"Saved summary ranking to: {summary_file}")

    print("\nPipeline finished successfully!")


if __name__ == "__main__":
    run_pipeline()