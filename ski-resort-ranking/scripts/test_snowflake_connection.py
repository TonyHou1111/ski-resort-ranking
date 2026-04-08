import os
from datetime import datetime, timezone

from dotenv import load_dotenv
import snowflake.connector


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_connection():
    return snowflake.connector.connect(
        account=get_required_env("SNOWFLAKE_ACCOUNT"),
        user=get_required_env("SNOWFLAKE_USER"),
        password=get_required_env("SNOWFLAKE_PASSWORD"),
        warehouse=get_required_env("SNOWFLAKE_WAREHOUSE"),
        database=get_required_env("SNOWFLAKE_DATABASE"),
        schema=get_required_env("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def main() -> None:
    load_dotenv()

    connection = get_connection()
    table_name = os.getenv("SNOWFLAKE_TEST_TABLE", "SKI_PIPELINE_CONNECTION_TEST")
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
