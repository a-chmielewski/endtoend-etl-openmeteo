import requests
import datetime as dt

BASE_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
PARAMS = {
    "timezone": "Europe/Berlin",
    "hourly": "temperature_2m,precipitation,wind_speed_10m"
}


def fetch_hourly_data(
    latitude: float, longitude: float, start_iso: str, end_iso: str
) -> dict:
    # Parse dates and format for API (YYYY-MM-DD)
    start = dt.datetime.fromisoformat(start_iso)
    end = dt.datetime.fromisoformat(end_iso)
    
    # Use start_date and end_date for historical data (not past_hours)
    params = PARAMS | {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
    }
    r = requests.get(BASE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_archive_data(
    latitude: float, longitude: float, start_date: str, end_date: str, timezone: str = "auto"
) -> dict:
    """
    Fetch historical data from the Archive API.
    
    Args:
        latitude: Location latitude
        longitude: Location longitude
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format (inclusive)
        timezone: Timezone (default: auto)
    
    Returns:
        JSON response from Archive API
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "timezone": timezone,
        "hourly": "temperature_2m,precipitation,wind_speed_10m"
    }
    r = requests.get(ARCHIVE_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()
