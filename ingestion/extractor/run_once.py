import datetime as dt
from openmeteo_client import fetch_hourly_data
from s3_writer import write_raw

# Configuration
CITY = "Warsaw"
LATITUDE = 52.23
LONGITUDE = 21.01

# Fetch last 6 hours of data
end = dt.datetime.now(dt.UTC).replace(minute=0, second=0, microsecond=0)
start = end - dt.timedelta(hours=6)

print(f"Fetching last 6 hours of data for {CITY} ({LATITUDE}, {LONGITUDE})")
print(f"Time range: {start} to {end}")

payload = fetch_hourly_data(LATITUDE, LONGITUDE, start.isoformat(), end.isoformat())

# Extract hourly arrays
hourly = payload.get("hourly", {})
times = hourly.get("time", [])
temps = hourly.get("temperature_2m", [])
precips = hourly.get("precipitation", [])
winds = hourly.get("wind_speed_10m", [])

if not times:
    print("No hourly data returned")
    exit(1)

print(f"Got {len(times)} hourly data points")

# Split into one file per hour
files_written = 0
for i, time_str in enumerate(times):
    # Parse timestamp
    hour_dt = dt.datetime.fromisoformat(time_str.replace("Z", "+00:00"))

    # Create single-hour payload
    single_hour_payload = {
        "latitude": payload.get("latitude"),
        "longitude": payload.get("longitude"),
        "timezone": payload.get("timezone"),
        "hourly": {
            "time": [time_str],
            "temperature_2m": [temps[i]] if i < len(temps) else [None],
            "precipitation": [precips[i]] if i < len(precips) else [None],
            "wind_speed_10m": [winds[i]] if i < len(winds) else [None],
        },
    }

    # Write with the actual hour for partitioning
    partition_dt = hour_dt.replace(tzinfo=None)
    key = write_raw(
        "raw", "weather", single_hour_payload, city=CITY, partition_dt=partition_dt
    )
    files_written += 1
    print(f"  ✓ Wrote to {key}")

print(f"\nTotal files written: {files_written}")
