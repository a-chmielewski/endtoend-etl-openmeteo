import requests
import datetime as dt

BASE_URL = "https://api.open-meteo.com/v1/forecast"
PARAMS = {
    "timezone": "Europe/Berlin",
    "hourly": "temperature_2m,precipitation,wind_speed_10m"
}


def fetch_hourly_data(
    latitude: float, longitude: float, start_iso: str, end_iso: str
) -> dict:
    # compute how many hours back from end -> start
    start = dt.datetime.fromisoformat(start_iso)
    end = dt.datetime.fromisoformat(end_iso)
    hours_back = max(0, int((end - start).total_seconds() // 3600))

    params = PARAMS | {
        "latitude": latitude,
        "longitude": longitude,
        "past_hours": hours_back,   # e.g., 6
        "forecast_hours": 0,        # only past, no future hours
    }
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()
