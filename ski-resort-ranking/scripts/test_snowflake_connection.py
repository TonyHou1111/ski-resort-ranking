import os
from datetime import datetime, timezone
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.storage.snowflake_utils import get_connection, load_environment, normalize_identifier


def main() -> None:
    load_environment()

    connection = get_connection()
    table_name = normalize_identifier(
        os.getenv("SNOWFLAKE_TEST_TABLE", "SKI_PIPELINE_CONNECTION_TEST")
    )
    inserted_at = datetime.now(timezone.utc).isoformat()

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            version, warehouse, database, schema = cursor.fetchone()
            print("Connected to Snowflake")
            print(f"Version: {version}")
            print(f"Warehouse: {warehouse}")
            print(f"Database: {database}")
            print(f"Schema: {schema}")

            # Create a lightweight write target so the test verifies more than authentication.
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    test_name STRING,
                    inserted_at STRING
                )
                """
            )

            cursor.execute(
                f"""
                INSERT INTO {table_name} (test_name, inserted_at)
                VALUES (%s, %s)
                """,
                ("python_connection_test", inserted_at),
            )

            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            print(f"Inserted 1 row into {table_name}")
            print(f"Current row count in {table_name}: {row_count}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
