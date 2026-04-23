import argparse
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ranking.compute_ranking import compute_ranking
from src.ranking.compute_summary_ranking import compute_summary_ranking
from src.storage.snowflake_utils import (
    fetch_table_row_count,
    load_environment,
    write_dataframe_to_table,
)


DEFAULT_TABLES = {
    "scored": ("SNOWFLAKE_SCORED_TABLE", "WEATHER_SCORED"),
    "summary": ("SNOWFLAKE_SUMMARY_TABLE", "SUMMARY_RANKING"),
    "ranking": ("SNOWFLAKE_RANKING_TABLE", "PIPELINE_RANKING"),
}


def get_scored_dataframe() -> pd.DataFrame:
    scored_path = PROJECT_ROOT / "data" / "processed" / "weather_scored.csv"
    if not scored_path.exists():
        raise FileNotFoundError(
            f"Missing scored data: {scored_path}. "
            "Run the pipeline or streaming producer first."
        )
    return pd.read_csv(scored_path)


def get_summary_dataframe(scored_df: pd.DataFrame) -> pd.DataFrame:
    return compute_summary_ranking(scored_df)


def get_ranking_dataframe(scored_df: pd.DataFrame) -> pd.DataFrame:
    ranking_input = scored_df.copy()
    ranking_input["event_time"] = pd.to_datetime(ranking_input["event_time"])
    return compute_ranking(ranking_input)


def get_target_table_name(source_name: str) -> str:
    env_name, default_name = DEFAULT_TABLES[source_name]
    return os.getenv(env_name, default_name)


def upload_dataset(source_name: str, df: pd.DataFrame, overwrite: bool) -> None:
    table_name = get_target_table_name(source_name)
    loaded_rows, chunk_count = write_dataframe_to_table(
        df,
        table_name,
        overwrite=overwrite,
    )
    total_rows = fetch_table_row_count(table_name)
    print(
        f"Loaded {loaded_rows} row(s) into {table_name} "
        f"using {chunk_count} chunk(s). Table now has {total_rows} row(s)."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load pipeline outputs into Snowflake tables.",
    )
    parser.add_argument(
        "--source",
        choices=["scored", "summary", "ranking", "all"],
        default="all",
        help="Which dataset to upload. Default: all",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace target table contents instead of appending.",
    )
    return parser.parse_args()


def main() -> None:
    load_environment()
    args = parse_args()

    scored_df = get_scored_dataframe()

    source_to_df = {
        "scored": scored_df,
        "summary": get_summary_dataframe(scored_df),
        "ranking": get_ranking_dataframe(scored_df),
    }

    sources = list(source_to_df) if args.source == "all" else [args.source]
    for source_name in sources:
        print(f"Uploading {source_name} dataset...")
        upload_dataset(source_name, source_to_df[source_name], overwrite=args.overwrite)


if __name__ == "__main__":
    main()
