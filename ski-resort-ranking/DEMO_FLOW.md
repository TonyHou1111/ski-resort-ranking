# Demo Flow

Use the real project directory:

```bash
cd /Users/tonyhou/PycharmProjects/ski-resort-ranking/ski-resort-ranking
```

## First Batch

### 1. Start Kafka

```bash
docker compose up -d kafka
```

### 2. Start Spark Structured Streaming

Open a new terminal and keep it running:

```bash
python3 src/streaming/spark_streaming_job.py \
  --source kafka \
  --bootstrap-servers localhost:9092
```

### 3. Run the first producer batch

Open another terminal:

```bash
python3 scripts/run_streaming_producer.py \
  --run-once \
  --bootstrap-servers localhost:9092
```

### 4. Load the latest Spark parquet snapshot into Snowflake

```bash
python3 scripts/load_streaming_summary_to_snowflake.py
python3 scripts/load_to_snowflake.py --source all --overwrite
```

### 5. Start the dashboard

Open another terminal:

```bash
streamlit run streamlit_app.py
```

## Second Batch

Keep Kafka, Spark, and the dashboard running.

### 1. Run the producer again

```bash
python3 scripts/run_streaming_producer.py \
  --run-once \
  --bootstrap-servers localhost:9092
```

### 2. Load the updated results into Snowflake

```bash
python3 scripts/load_streaming_summary_to_snowflake.py
python3 scripts/load_to_snowflake.py --source all --overwrite
```

### 3. Refresh the dashboard

In the dashboard sidebar, click:

`Refresh Data`

## Scheduled Producer Demo

After showing two manual batches, run the producer in loop mode:

```bash
python3 scripts/run_streaming_producer.py \
  --bootstrap-servers localhost:9092
```

Current behavior:

- default interval is 5 minutes
- each cycle fetches real weather data
- each cycle publishes a new scored batch to Kafka

## Stop Everything

```bash
docker compose down
pkill -f 'spark_streaming_job.py'
pkill -f 'streamlit_app.py'
pkill -f 'run_streaming_producer.py'
rm -rf tmp/checkpoint_streaming
rm -rf data/out/streaming_summary
```
