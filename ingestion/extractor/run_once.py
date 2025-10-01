import os
import datetime as dt
from openmeteo_client import fetch_hourly_data
from s3_writer import write_raw

latitude, longitude = 52.52, 13.41
end = dt.datetime.now(dt.UTC).replace(minute=0, second=0, microsecond=0)
start = end - dt.timedelta(hours=6)
payload = fetch_hourly_data(latitude, longitude, start.isoformat(), end.isoformat())
key = write_raw("raw", "weather", payload)
print(f"Wrote to {key}")