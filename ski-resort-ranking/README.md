# Ski Resort Ranking

Pipeline project for fetching weather data, cleaning it, and generating ski resort rankings.

## Structure

- `data/raw/`: raw API responses
- `data/processed/`: cleaned datasets
- `data/final/`: ranking outputs
- `src/fetch/`: data ingestion
- `src/transform/`: data cleaning
- `src/storage/`: storage utilities
- `src/ranking/`: ranking logic
- `src/utils/`: shared helpers
- `scripts/`: pipeline entrypoints
- `notebooks/`: analysis and exploration

## Snowflake

1. Create a `.env` file from `.env.example` and fill in your Snowflake credentials.
2. Test the Snowflake connection:

   ```bash
   python3 scripts/test_snowflake_connection.py
   ```

3. Load pipeline outputs into Snowflake:

   ```bash
   python3 scripts/load_to_snowflake.py --source all --overwrite
   ```

4. Load Spark streaming parquet output into Snowflake:

   ```bash
   python3 scripts/load_streaming_summary_to_snowflake.py
   ```

This uploads:

- `weather_scored.csv` -> `WEATHER_SCORED`
- summary ranking -> `SUMMARY_RANKING`
- event-time ranking -> `PIPELINE_RANKING`
- `data/out/streaming_summary` -> `STREAMING_SUMMARY`

## Dashboard

Run the Streamlit dashboard:

```bash
streamlit run streamlit_app.py
```

The dashboard reads from Snowflake and uses:

- `STREAMING_SUMMARY` for the latest streaming snapshot
- `SUMMARY_RANKING` for overall ranking
- `WEATHER_SCORED` for time-series trends
