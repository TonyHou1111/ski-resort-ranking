import argparse
import os
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.storage.snowflake_utils import (
    fetch_table_row_count,
    load_environment,
    write_dataframe_to_table,
)


DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "out" / "streaming_summary"
DEFAULT_TABLE_ENV = "SNOWFLAKE_STREAMING_SUMMARY_TABLE"
DEFAULT_TABLE_NAME = "STREAMING_SUMMARY"


def get_target_table_name() -> str:
    return os.getenv(DEFAULT_TABLE_ENV, DEFAULT_TABLE_NAME)


def load_streaming_summary(input_dir: Path) -> pd.DataFrame:
    if not input_dir.exists():
        raise FileNotFoundError(
            f"Streaming summary directory not found: {input_dir}. "
            "Run the Spark streaming job with parquet output first."
        )

    # Spark writes a directory of parquet parts; pandas can read the whole snapshot at once.
    df = pd.read_parquet(input_dir)
    if df.empty:
        raise ValueError(f"No rows found in streaming summary parquet: {input_dir}")
    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Spark streaming parquet output into a Snowflake table.",
    )
    parser.add_argument(
        "--input-dir",
        default=str(DEFAULT_INPUT_DIR),
        help="Directory containing the streaming parquet snapshot. Default: data/out/streaming_summary",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to the target table instead of replacing it. Default: replace table contents.",
    )
    return parser.parse_args()


def main() -> None:
    load_environment()
    args = parse_args()
    input_dir = Path(args.input_dir)

    df = load_streaming_summary(input_dir)
    table_name = get_target_table_name()
    overwrite = not args.append

    loaded_rows, chunk_count = write_dataframe_to_table(
        df,
        table_name,
        overwrite=overwrite,
    )
    total_rows = fetch_table_row_count(table_name)
    action = "Replaced" if overwrite else "Appended to"
    print(
        f"{action} {table_name} with {loaded_rows} row(s) from {input_dir} "
        f"using {chunk_count} chunk(s). Table now has {total_rows} row(s)."
    )


if __name__ == "__main__":
    main()
