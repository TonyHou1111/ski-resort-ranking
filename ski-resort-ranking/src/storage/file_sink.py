import json
from datetime import datetime, timezone
from pathlib import Path


def write_jsonl_batch(
    records: list[dict],
    output_dir: str | Path,
    prefix: str = "weather_batch",
) -> Path:
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)

    # Timestamped files let Spark pick up each batch without overwriting prior inputs.
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_file = path / f"{prefix}_{timestamp}.jsonl"

    with output_file.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record))
            handle.write("\n")

    print(f"Wrote {len(records)} records to {output_file}")
    return output_file
