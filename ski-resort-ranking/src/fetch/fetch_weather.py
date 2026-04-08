import json
from datetime import datetime, timezone
from pathlib import Path

import requests


RESORTS = [
    {"resort_name": "Aspen Snowmass", "latitude": 39.1911, "longitude": -106.8175},
    {"resort_name": "Vail", "latitude": 39.6403, "longitude": -106.3742},
    {"resort_name": "Breckenridge", "latitude": 39.4817, "longitude": -106.0384},
    {"resort_name": "Park City", "latitude": 40.6461, "longitude": -111.4980},
    {"resort_name": "Jackson Hole", "latitude": 43.5875, "longitude": -110.8272},
]


def fetch_one_resort_weather(resort: dict) -> dict:
    lat = resort["latitude"]
    lon = resort["longitude"]

    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}"
        f"&longitude={lon}"
        f"&hourly=snowfall,temperature_2m"
    )

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    result = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "resort_name": resort["resort_name"],
        "latitude": lat,
        "longitude": lon,
        "raw_data": data,
    }

    return result


def fetch_weather_data() -> list:
    all_results = []

    for resort in RESORTS:
        print(f"Fetching data for {resort['resort_name']}...")
        resort_data = fetch_one_resort_weather(resort)
        all_results.append(resort_data)

    return all_results


def save_raw_data(data: list):
    project_root = Path(__file__).resolve().parents[2]
    output_dir = project_root / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"weather_{timestamp}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Saved raw data to: {output_file}")


if __name__ == "__main__":
    weather_data = fetch_weather_data()
    save_raw_data(weather_data)