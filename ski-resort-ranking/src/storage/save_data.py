"""Storage helpers for pipeline outputs."""

from pathlib import Path
import json


def save_json(data: list[dict], output_path: str) -> None:
    """Save records to a JSON file."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
