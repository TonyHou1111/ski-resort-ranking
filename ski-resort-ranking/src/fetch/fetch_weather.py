import json
from datetime import datetime, timezone
from pathlib import Path

import requests


HOURLY_VARIABLES = [
    "snowfall",
    "temperature_2m",
    "wind_speed_10m",
    "snow_depth",
]

RESORTS = [
    {"resort_name": "Aspen Snowmass", "latitude": 39.1911, "longitude": -106.8175},
    {"resort_name": "Vail", "latitude": 39.6403, "longitude": -106.3742},
    {"resort_name": "Breckenridge", "latitude": 39.4817, "longitude": -106.0384},
    {"resort_name": "Park City", "latitude": 40.6461, "longitude": -111.4980},
    {"resort_name": "Jackson Hole", "latitude": 43.5875, "longitude": -110.8272},
    {"resort_name": "Alta", "latitude": 40.5883, "longitude": -111.6377},
    {"resort_name": "Snowbird", "latitude": 40.5819, "longitude": -111.6550},
    {"resort_name": "Deer Valley", "latitude": 40.6193, "longitude": -111.4780},
    {"resort_name": "Keystone", "latitude": 39.5792, "longitude": -105.9347},
    {"resort_name": "Beaver Creek", "latitude": 39.6042, "longitude": -106.5167},
    {"resort_name": "Winter Park", "latitude": 39.8860, "longitude": -105.7625},
    {"resort_name": "Steamboat", "latitude": 40.4850, "longitude": -106.8317},
    {"resort_name": "Telluride", "latitude": 37.9367, "longitude": -107.8466},
    {"resort_name": "Mammoth Mountain", "latitude": 37.6308, "longitude": -119.0326},
    {"resort_name": "Heavenly", "latitude": 38.9350, "longitude": -119.9400},
    {"resort_name": "Palisades Tahoe", "latitude": 39.1960, "longitude": -120.2357},
    {"resort_name": "Big Sky", "latitude": 45.2847, "longitude": -111.4018},
    {"resort_name": "Sun Valley", "latitude": 43.6971, "longitude": -114.3517},
    {"resort_name": "Copper Mountain", "latitude": 39.5022, "longitude": -106.1519},
    {"resort_name": "Taos Ski Valley", "latitude": 36.5945, "longitude": -105.4547},
]


def fetch_one_resort_weather(resort: dict) -> dict:
    lat = resort["latitude"]
    lon = resort["longitude"]

    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}"
        f"&longitude={lon}"
        f"&hourly={','.join(HOURLY_VARIABLES)}"
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


def save_raw_data(data: list, output_dir: Path | None = None) -> Path:
    project_root = Path(__file__).resolve().parents[2]
    output_dir = output_dir or (project_root / "data" / "raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"weather_{timestamp}.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Saved raw data to: {output_file}")
    return output_file


if __name__ == "__main__":
    weather_data = fetch_weather_data()
    save_raw_data(weather_data)
