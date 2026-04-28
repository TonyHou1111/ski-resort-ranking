# Ski Resort Ranking

This project fetches ski resort weather data from Open-Meteo, cleans and scores it, builds resort rankings, and optionally pushes the results into Snowflake for a Streamlit dashboard.

## What You Can Run

There are 3 common ways to use this repo:

1. **Local batch pipeline**
   Run the data pipeline end to end and generate CSV outputs locally. This is the easiest way to verify the project works.
2. **Snowflake + Streamlit dashboard**
   Upload pipeline outputs to Snowflake and view them in the dashboard.
3. **Streaming demo**
   Simulate streaming batches with Spark. You can use the simpler file-based mode or the Kafka mode.

## Prerequisites

- Python 3.10+
- Internet access to call the Open-Meteo API
- Optional: Docker, if you want to use Kafka
- Optional: Java 17 or 21, if you want to run the Spark streaming job
- Optional: Snowflake account and credentials, if you want to load data into Snowflake or run the dashboard

## Project Structure

- `data/raw/`: raw API responses
- `data/processed/`: cleaned, normalized, and scored CSV files
- `data/final/`: final ranking outputs
- `data/landing/`: file-based streaming input batches
- `data/out/streaming_summary/`: Spark streaming summary parquet output
- `scripts/`: batch pipeline, Snowflake loaders, and producer scripts
- `src/fetch/`: weather data ingestion
- `src/transform/`: cleaning and normalization
- `src/ranking/`: scoring and ranking logic
- `src/streaming/`: Spark Structured Streaming job
- `src/storage/`: Snowflake, Kafka, and file output helpers
- `streamlit_app.py`: Streamlit dashboard entrypoint

## Quick Start

If you only want to confirm the project runs, use this path first.

### 1. Enter the project directory

If you cloned the outer repository root, move into the actual app directory first:

```bash
cd ski-resort-ranking
```

### 2. Create and activate a virtual environment

macOS / Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run the batch pipeline

```bash
python3 scripts/run_pipeline.py
```

What this command does:

- fetches weather data for the configured ski resorts
- cleans and normalizes the data
- computes resort scores
- writes final ranking files locally

Expected output files:

- `data/raw/weather_*.json`
- `data/processed/weather_cleaned.csv`
- `data/processed/weather_normalized.csv`
- `data/processed/weather_scored.csv`
- `data/final/pipeline_ranking.csv`
- `data/final/summary_ranking.csv`

## Run the Dashboard

The dashboard reads from Snowflake. It does **not** read local CSV files directly.

### 1. Create a `.env` file

```bash
cp .env.example .env
```

Then fill in your Snowflake configuration.

Required base fields:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`

Authentication:

- Set `SNOWFLAKE_AUTH_METHOD=password` and fill `SNOWFLAKE_PASSWORD`
- Or set `SNOWFLAKE_AUTH_METHOD=key_pair` and provide the private key path and passphrase settings

### 2. Test the Snowflake connection

```bash
python3 scripts/test_snowflake_connection.py
```

### 3. Upload batch outputs to Snowflake

Run the local batch pipeline first if you have not generated `data/processed/weather_scored.csv` yet.

```bash
python3 scripts/load_to_snowflake.py --source all --overwrite
```

This uploads:

- `weather_scored.csv` -> `WEATHER_SCORED`
- summary ranking -> `SUMMARY_RANKING`
- event-time ranking -> `PIPELINE_RANKING`

### 4. Make sure `STREAMING_SUMMARY` exists

The dashboard also queries `STREAMING_SUMMARY`.

You have two choices:

- load the sample parquet snapshot already included in `data/out/streaming_summary/`
- or regenerate it with the streaming demo first

To load the current parquet snapshot into Snowflake:

```bash
python3 scripts/load_streaming_summary_to_snowflake.py
```

### 5. Start the dashboard

```bash
streamlit run streamlit_app.py
```

The dashboard uses:

- `STREAMING_SUMMARY` for the latest streaming snapshot
- `SUMMARY_RANKING` for overall resort ranking
- `WEATHER_SCORED` for the trends view

## Run the Streaming Demo

You have two choices:

1. **File mode**
   Easiest to run locally. No Kafka or Docker required.
2. **Kafka mode**
   Closer to a real streaming pipeline, but needs Docker and Kafka.

### Option A: File-Based Streaming

Start the Spark streaming job in one terminal:

```bash
python3 src/streaming/spark_streaming_job.py \
  --source file \
  --input-dir data/landing \
  --output-mode parquet
```

Then generate one batch in a second terminal:

```bash
python3 scripts/run_streaming_producer.py \
  --run-once \
  --output-mode file \
  --landing-dir data/landing
```

Result:

- scored records are written to `data/landing/`
- Spark aggregates them and writes parquet snapshots to `data/out/streaming_summary/`

If you want to load the latest streaming snapshot into Snowflake:

```bash
python3 scripts/load_streaming_summary_to_snowflake.py
```

### Option B: Kafka Streaming

Start Kafka:

```bash
docker compose up -d kafka
```

Start the Spark streaming job in another terminal:

```bash
python3 src/streaming/spark_streaming_job.py \
  --source kafka \
  --bootstrap-servers localhost:9092
```

Send one producer batch in a third terminal:

```bash
python3 scripts/run_streaming_producer.py \
  --run-once \
  --bootstrap-servers localhost:9092
```

To keep sending batches on a schedule:

```bash
python3 scripts/run_streaming_producer.py \
  --bootstrap-servers localhost:9092
```

Default behavior:

- polling interval is 5 minutes
- the Kafka topic is `ski_weather_scored`
- Spark writes aggregated parquet snapshots to `data/out/streaming_summary/`

### Load Streaming Output into Snowflake

After the Spark job creates parquet output, run:

```bash
python3 scripts/load_streaming_summary_to_snowflake.py
```

## Recommended First-Time Flow

If someone is running this repo for the first time, this order is the least painful:

1. `pip install -r requirements.txt`
2. `python3 scripts/run_pipeline.py`
3. Optional: `python3 scripts/test_snowflake_connection.py`
4. Optional: `python3 scripts/load_to_snowflake.py --source all --overwrite`
5. Optional: `python3 scripts/load_streaming_summary_to_snowflake.py`
6. Optional: `streamlit run streamlit_app.py`
7. Optional: try the streaming demo in file mode before Kafka mode

## Troubleshooting

### `python3: command not found`

Use `python` instead of `python3`, or install Python 3.10+ first.

### `ModuleNotFoundError`

Make sure the virtual environment is activated and dependencies were installed:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### `Java runtime not found`

The Spark streaming job needs Java. Install OpenJDK 17 or 21 and set `JAVA_HOME`, or add `java` to your `PATH`.

### `Failed to load dashboard data from Snowflake`

Check these items:

- `.env` exists in the project root
- Snowflake credentials are correct
- the target tables have already been loaded, including `STREAMING_SUMMARY`
- your chosen auth method matches the fields you filled in

### Kafka starts, but no streaming data appears

Check these items:

- `docker compose up -d kafka` completed successfully
- Spark is running with `--source kafka`
- the producer is sending to `localhost:9092`
- the Spark checkpoint directory is not holding old state you no longer want

If needed, stop the processes and clear the checkpoint:

```bash
rm -rf tmp/checkpoint_streaming
```

## Useful Commands

Run one producer batch without Kafka:

```bash
python3 scripts/run_streaming_producer.py --run-once --output-mode file
```

Run one producer batch with Kafka:

```bash
python3 scripts/run_streaming_producer.py --run-once --bootstrap-servers localhost:9092
```

See all streaming job options:

```bash
python3 src/streaming/spark_streaming_job.py --help
```
