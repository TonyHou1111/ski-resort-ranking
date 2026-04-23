import os
import re
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector import SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas


PROJECT_ROOT = Path(__file__).resolve().parents[2]
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SUPPORTED_AUTH_METHODS = {"password", "key_pair"}


def load_environment() -> None:
    load_dotenv(PROJECT_ROOT / ".env")


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def normalize_identifier(identifier: str) -> str:
    candidate = identifier.strip().upper()
    if not IDENTIFIER_PATTERN.fullmatch(candidate):
        raise ValueError(
            f"Invalid Snowflake identifier: {identifier!r}. "
            "Use letters, numbers, and underscores only."
        )
    return candidate


def get_auth_method() -> str:
    auth_method = os.getenv("SNOWFLAKE_AUTH_METHOD", "password").strip().lower()
    if auth_method not in SUPPORTED_AUTH_METHODS:
        raise ValueError(
            f"Unsupported SNOWFLAKE_AUTH_METHOD={auth_method!r}. "
            f"Use one of: {', '.join(sorted(SUPPORTED_AUTH_METHODS))}."
        )
    return auth_method


def get_private_key_file() -> str:
    path = Path(get_required_env("SNOWFLAKE_PRIVATE_KEY_FILE")).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Snowflake private key file not found: {path}")
    return str(path)


def get_private_key_passphrase() -> str | None:
    if passphrase := os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
        return passphrase

    passphrase_file = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE_FILE")
    if not passphrase_file:
        return None

    path = Path(passphrase_file).expanduser()
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Snowflake private key passphrase file not found: {path}")
    return path.read_text(encoding="utf-8").strip()


def get_connection_params() -> dict:
    params = {
        "account": get_required_env("SNOWFLAKE_ACCOUNT"),
        "user": get_required_env("SNOWFLAKE_USER"),
        "warehouse": get_required_env("SNOWFLAKE_WAREHOUSE"),
        "database": get_required_env("SNOWFLAKE_DATABASE"),
        "schema": get_required_env("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }

    auth_method = get_auth_method()
    if auth_method == "key_pair":
        params.update({
            "authenticator": "SNOWFLAKE_JWT",
            "private_key_file": get_private_key_file(),
        })
        private_key_passphrase = get_private_key_passphrase()
        if private_key_passphrase:
            params["private_key_file_pwd"] = private_key_passphrase
    else:
        params["password"] = get_required_env("SNOWFLAKE_PASSWORD")

    return params


def get_connection() -> SnowflakeConnection:
    load_environment()
    return snowflake.connector.connect(**get_connection_params())


def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized_df = df.copy()
    normalized_df.columns = [normalize_identifier(str(column)) for column in normalized_df.columns]
    return normalized_df


def write_dataframe_to_table(
    df: pd.DataFrame,
    table_name: str,
    *,
    overwrite: bool = False,
    database: str | None = None,
    schema: str | None = None,
) -> tuple[int, int]:
    if df.empty:
        raise ValueError("Cannot write an empty DataFrame to Snowflake.")

    normalized_df = normalize_dataframe_columns(df)
    normalized_table = normalize_identifier(table_name)

    connection = get_connection()
    try:
        success, nchunks, nrows, _ = write_pandas(
            connection,
            normalized_df,
            table_name=normalized_table,
            database=database,
            schema=schema,
            auto_create_table=True,
            overwrite=overwrite,
            quote_identifiers=False,
            infer_schema=True,
        )
        if not success:
            raise RuntimeError(f"write_pandas reported failure for table {normalized_table}.")
        return nrows, nchunks
    finally:
        connection.close()


def fetch_table_row_count(table_name: str) -> int:
    normalized_table = normalize_identifier(table_name)
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {normalized_table}")
            return int(cursor.fetchone()[0])
    finally:
        connection.close()


def query_dataframe(sql: str, params: tuple | None = None) -> pd.DataFrame:
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, params=params)
            return cursor.fetch_pandas_all()
    finally:
        connection.close()
