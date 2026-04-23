import os
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
DISPLAY_TIMEZONE = "America/Chicago"

from src.storage.snowflake_utils import load_environment, query_dataframe


DEFAULT_STREAMING_TABLE = "STREAMING_SUMMARY"
DEFAULT_SUMMARY_TABLE = "SUMMARY_RANKING"
DEFAULT_SCORED_TABLE = "WEATHER_SCORED"


def get_table_name(env_name: str, default_name: str) -> str:
    return os.getenv(env_name, default_name)


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(168, 218, 220, 0.35), transparent 28%),
                radial-gradient(circle at top right, rgba(69, 123, 157, 0.18), transparent 24%),
                linear-gradient(180deg, #f4f8fb 0%, #eef3f7 100%);
        }
        .stApp, .stApp p, .stApp label, .stApp h1, .stApp h2, .stApp h3, .stApp h4 {
            color: #17324d;
        }
        .hero {
            padding: 1.2rem 1.4rem;
            border-radius: 18px;
            background: linear-gradient(135deg, rgba(233, 242, 248, 0.96), rgba(214, 229, 239, 0.92));
            color: #17324d;
            border: 1px solid rgba(69, 123, 157, 0.18);
            box-shadow: 0 18px 40px rgba(29, 53, 87, 0.08);
            margin-bottom: 1rem;
        }
        .hero h1 {
            margin: 0;
            font-size: 2.3rem;
            letter-spacing: 0.02em;
        }
        .hero p {
            margin: 0.4rem 0 0;
            font-size: 1rem;
            color: #315674;
        }
        .metric-label {
            text-transform: uppercase;
            font-size: 0.78rem;
            letter-spacing: 0.08em;
            color: #577590;
        }
        [data-testid="stMetricValue"] {
            color: #17324d;
        }
        [data-testid="stMetricLabel"] {
            color: #4f6b85;
        }
        [data-testid="stSidebar"] * {
            color: #17324d;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def convert_dataframe_times(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    converted_df = df.copy()
    for column in columns:
        converted_df[column] = (
            pd.to_datetime(converted_df[column], errors="coerce", utc=True)
            .dt.tz_convert(DISPLAY_TIMEZONE)
        )
    return converted_df


@st.cache_data(ttl=30, show_spinner=False)
def load_latest_streaming_snapshot() -> pd.DataFrame:
    table_name = get_table_name("SNOWFLAKE_STREAMING_SUMMARY_TABLE", DEFAULT_STREAMING_TABLE)
    query = f"""
        WITH latest_snapshot AS (
            SELECT MAX(SNAPSHOT_BATCH_ID) AS SNAPSHOT_BATCH_ID
            FROM {table_name}
        )
        SELECT
            RESORT_NAME,
            AVG_SCORE,
            TOTAL_SNOWFALL,
            AVG_TEMPERATURE,
            RECORDS,
            WINDOW_START,
            WINDOW_END,
            SNAPSHOT_BATCH_ID,
            SNAPSHOT_WRITTEN_AT
        FROM {table_name}
        WHERE SNAPSHOT_BATCH_ID = (SELECT SNAPSHOT_BATCH_ID FROM latest_snapshot)
        ORDER BY AVG_SCORE DESC, RESORT_NAME
    """
    df = query_dataframe(query)
    if not df.empty:
        df = convert_dataframe_times(df, ["WINDOW_START", "WINDOW_END", "SNAPSHOT_WRITTEN_AT"])
    return df


@st.cache_data(ttl=60, show_spinner=False)
def load_summary_ranking() -> pd.DataFrame:
    table_name = get_table_name("SNOWFLAKE_SUMMARY_TABLE", DEFAULT_SUMMARY_TABLE)
    query = f"""
        SELECT
            RANK,
            RESORT_NAME,
            AVG_SCORE,
            TOTAL_SNOWFALL,
            AVG_TEMPERATURE,
            RECORDS
        FROM {table_name}
        ORDER BY RANK
    """
    return query_dataframe(query)


@st.cache_data(ttl=60, show_spinner=False)
def load_score_history() -> pd.DataFrame:
    table_name = get_table_name("SNOWFLAKE_SCORED_TABLE", DEFAULT_SCORED_TABLE)
    query = f"""
        SELECT
            EVENT_TIME,
            RESORT_NAME,
            AVG(OVERALL_SCORE) AS AVG_SCORE,
            SUM(SNOWFALL) AS TOTAL_SNOWFALL,
            AVG(TEMPERATURE) AS AVG_TEMPERATURE
        FROM {table_name}
        GROUP BY EVENT_TIME, RESORT_NAME
        ORDER BY EVENT_TIME, RESORT_NAME
    """
    df = query_dataframe(query)
    if not df.empty:
        df = convert_dataframe_times(df, ["EVENT_TIME"])
    return df


def render_overview(streaming_df: pd.DataFrame, summary_df: pd.DataFrame) -> None:
    if streaming_df.empty:
        st.warning("`STREAMING_SUMMARY` is empty. Run the Spark parquet loader first.")
        return

    latest_row = streaming_df.iloc[0]
    top_resort = latest_row["RESORT_NAME"]
    window_start = latest_row["WINDOW_START"]
    window_end = latest_row["WINDOW_END"]
    snapshot_written_at = latest_row["SNAPSHOT_WRITTEN_AT"]

    st.markdown(
        f"""
        <div class="hero">
            <h1>Ski Resort Command Center</h1>
            <p>
                Live snapshot from Snowflake. Current window ({DISPLAY_TIMEZONE}):
                {window_start:%Y-%m-%d %H:%M} to {window_end:%H:%M}.
                Latest write ({DISPLAY_TIMEZONE}): {snapshot_written_at:%Y-%m-%d %H:%M:%S}.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Top Resort", top_resort)
    col2.metric("Best Avg Score", f"{latest_row['AVG_SCORE']:.2f}")
    col3.metric("Tracked Resorts", f"{len(streaming_df)}")
    col4.metric("Window Snowfall", f"{streaming_df['TOTAL_SNOWFALL'].sum():.2f}")

    st.subheader("Current Streaming Snapshot")
    snapshot_chart = (
        streaming_df[["RESORT_NAME", "AVG_SCORE"]]
        .rename(columns={"RESORT_NAME": "Resort", "AVG_SCORE": "Average Score"})
        .set_index("Resort")
    )
    st.bar_chart(snapshot_chart, color="#457b9d")

    display_df = streaming_df.rename(columns={
        "RESORT_NAME": "Resort",
        "AVG_SCORE": "Avg Score",
        "TOTAL_SNOWFALL": "Total Snowfall",
        "AVG_TEMPERATURE": "Avg Temperature",
        "RECORDS": "Records",
        "WINDOW_START": "Window Start",
        "WINDOW_END": "Window End",
        "SNAPSHOT_BATCH_ID": "Snapshot Batch",
        "SNAPSHOT_WRITTEN_AT": "Snapshot Written At",
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.subheader("Summary Ranking")
    st.dataframe(summary_df, use_container_width=True, hide_index=True)


def render_trends(history_df: pd.DataFrame) -> None:
    st.subheader("Score Trends")
    if history_df.empty:
        st.warning("`WEATHER_SCORED` is empty. Load scored data into Snowflake first.")
        return

    resort_options = sorted(history_df["RESORT_NAME"].dropna().unique().tolist())
    selected_resorts = st.multiselect(
        "Select resorts",
        resort_options,
        default=resort_options[: min(3, len(resort_options))],
    )

    filtered_df = history_df[history_df["RESORT_NAME"].isin(selected_resorts)].copy()
    if filtered_df.empty:
        st.info("Select at least one resort to view trends.")
        return

    line_df = (
        filtered_df
        .pivot(index="EVENT_TIME", columns="RESORT_NAME", values="AVG_SCORE")
        .sort_index()
    )
    st.line_chart(line_df)

    snowfall_df = (
        filtered_df
        .pivot(index="EVENT_TIME", columns="RESORT_NAME", values="TOTAL_SNOWFALL")
        .sort_index()
    )
    st.caption("Total snowfall by event time")
    st.area_chart(snowfall_df)


def render_comparison(summary_df: pd.DataFrame) -> None:
    st.subheader("Resort Comparison")
    if summary_df.empty:
        st.warning("`SUMMARY_RANKING` is empty. Load summary ranking into Snowflake first.")
        return

    resorts = summary_df["RESORT_NAME"].tolist()
    default_right_index = 1 if len(resorts) > 1 else 0
    left_resort = st.selectbox("Left resort", resorts, index=0)
    right_resort = st.selectbox("Right resort", resorts, index=default_right_index)

    left_row = summary_df[summary_df["RESORT_NAME"] == left_resort].iloc[0]
    right_row = summary_df[summary_df["RESORT_NAME"] == right_resort].iloc[0]

    left_col, right_col = st.columns(2)
    with left_col:
        st.markdown(f"#### {left_resort}")
        st.metric("Rank", int(left_row["RANK"]))
        st.metric("Avg Score", f"{left_row['AVG_SCORE']:.2f}")
        st.metric("Total Snowfall", f"{left_row['TOTAL_SNOWFALL']:.2f}")
        st.metric("Avg Temperature", f"{left_row['AVG_TEMPERATURE']:.2f}")
        st.metric("Records", int(left_row["RECORDS"]))

    with right_col:
        st.markdown(f"#### {right_resort}")
        st.metric("Rank", int(right_row["RANK"]))
        st.metric("Avg Score", f"{right_row['AVG_SCORE']:.2f}")
        st.metric("Total Snowfall", f"{right_row['TOTAL_SNOWFALL']:.2f}")
        st.metric("Avg Temperature", f"{right_row['AVG_TEMPERATURE']:.2f}")
        st.metric("Records", int(right_row["RECORDS"]))


def main() -> None:
    load_environment()
    st.set_page_config(
        page_title="Ski Resort Dashboard",
        layout="wide",
    )
    inject_styles()

    st.sidebar.title("Controls")
    if st.sidebar.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("Snowflake-backed dashboard for ranking and streaming summary tables.")

    try:
        streaming_df = load_latest_streaming_snapshot()
        summary_df = load_summary_ranking()
        history_df = load_score_history()
    except Exception as exc:
        st.error(f"Failed to load dashboard data from Snowflake: {exc}")
        st.stop()

    overview_tab, trends_tab, compare_tab = st.tabs(["Overview", "Trends", "Compare"])

    with overview_tab:
        render_overview(streaming_df, summary_df)

    with trends_tab:
        render_trends(history_df)

    with compare_tab:
        render_comparison(summary_df)


if __name__ == "__main__":
    main()
