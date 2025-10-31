"""
Script to fetch weather data for every day of October 2025.
This script fetches historical data from OpenMeteo Archive API and stores it in MinIO.
"""
import datetime as dt
import pytz
from openmeteo_client import fetch_archive_data
from s3_writer import write_raw

# Configuration
LATITUDE = 52.52  # Berlin coordinates (default)
LONGITUDE = 13.41
CITY_NAME = "Berlin"

# You can also configure other cities:
CITIES = {
    "Berlin": (52.52, 13.41),
    "Warsaw": (52.23, 21.01),
    "London": (51.51, -0.13),
    "Paris": (48.85, 2.35),
}


def fetch_october_2025_data(city: str = None, latitude: float = None, longitude: float = None):
    """
    Fetch weather data from October 1st 2025 to now.
    
    Args:
        city: City name (will use predefined coordinates)
        latitude: Custom latitude (overrides city)
        longitude: Custom longitude (overrides city)
    """
    # Determine coordinates
    if latitude is not None and longitude is not None:
        lat, lon = latitude, longitude
        city_label = city or "Custom"
    elif city and city in CITIES:
        lat, lon = CITIES[city]
        city_label = city
    else:
        lat, lon = LATITUDE, LONGITUDE
        city_label = CITY_NAME
    
    print(f"Fetching October 2025 data for {city_label} ({lat}, {lon})")
    
    # October 1st 2025 00:00 to October 31st 2025 12:00 Warsaw time
    warsaw_tz = pytz.timezone('Europe/Warsaw')
    start_date = dt.datetime(2025, 10, 1, 0, 0, 0)
    end_datetime = warsaw_tz.localize(dt.datetime(2025, 10, 31, 12, 0, 0))
    
    # Convert to date for comparison
    end_date = end_datetime.date()
    
    # Fetch in chunks (one day at a time for better control and error handling)
    current_date = start_date.date()
    total_files = 0
    failed_dates = []
    
    while current_date <= end_date:
        try:
            print(f"Fetching data for {current_date}...")
            
            # Fetch exactly one calendar day (Archive API end_date is inclusive)
            payload = fetch_archive_data(
                latitude=lat,
                longitude=lon,
                start_date=current_date.strftime("%Y-%m-%d"),
                end_date=current_date.strftime("%Y-%m-%d"),
                timezone="auto"
            )
            
            # Extract hourly arrays
            hourly = payload.get("hourly", {})
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            precips = hourly.get("precipitation", [])
            winds = hourly.get("wind_speed_10m", [])
            
            if not times:
                print(f"  ⚠ No hourly data returned for {current_date}")
                continue
            
            # Split into one file per hour
            hours_written = 0
            for i, time_str in enumerate(times):
                # Parse timestamp
                hour_dt = dt.datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                
                # For Oct 31, only include hours up to 12:00 Warsaw time
                if current_date.day == 31 and current_date.month == 10:
                    # Convert to Warsaw time for comparison
                    hour_local = hour_dt.astimezone(warsaw_tz)
                    if hour_local.hour >= 12:
                        continue
                
                # Create single-hour payload
                single_hour_payload = {
                    "latitude": payload.get("latitude"),
                    "longitude": payload.get("longitude"),
                    "timezone": payload.get("timezone"),
                    "hourly": {
                        "time": [time_str],
                        "temperature_2m": [temps[i]] if i < len(temps) else [None],
                        "precipitation": [precips[i]] if i < len(precips) else [None],
                        "wind_speed_10m": [winds[i]] if i < len(winds) else [None]
                    }
                }
                
                # Write with the actual hour for partitioning
                partition_dt = hour_dt.replace(tzinfo=None)
                write_raw("raw", "weather", single_hour_payload, city=city_label, partition_dt=partition_dt)
                hours_written += 1
            
            total_files += hours_written
            print(f"  ✓ Wrote {hours_written} hourly files for {current_date}")
            
        except Exception as e:
            print(f"  ✗ Failed to fetch data for {current_date}: {e}")
            failed_dates.append(current_date)
        
        # Move to next day
        current_date += dt.timedelta(days=1)
    
    # Summary
    print("\n" + "="*60)
    print(f"Fetch completed for {city_label}")
    print(f"Total files written: {total_files}")
    
    if failed_dates:
        print(f"\nFailed dates ({len(failed_dates)}):")
        for date in failed_dates:
            print(f"  - {date}")
        print("\nNote: Check your network connection or API rate limits.")
        print("The Archive API is free but may have rate limiting.")
    else:
        print("All dates fetched successfully!")
    
    print("="*60)


def fetch_october_2025_multiple_cities():
    """Fetch October 2025 data for all configured cities."""
    print("Fetching October 2025 data for multiple cities...\n")
    
    for city, (lat, lon) in CITIES.items():
        print(f"\n{'='*60}")
        print(f"Processing: {city}")
        print('='*60)
        fetch_october_2025_data(city=city, latitude=lat, longitude=lon)


if __name__ == "__main__":
    import sys
    
    # Check command line arguments
    if len(sys.argv) > 1:
        city_arg = sys.argv[1]
        if city_arg.lower() == "all":
            # Fetch for all cities
            fetch_october_2025_multiple_cities()
        elif city_arg in CITIES:
            # Fetch for specific city
            fetch_october_2025_data(city=city_arg)
        else:
            print(f"Unknown city: {city_arg}")
            print(f"Available cities: {', '.join(CITIES.keys())}")
            print("Or use 'all' to fetch for all cities")
            sys.exit(1)
    else:
        # Default: fetch for all cities (Warsaw, Berlin, London, Paris)
        fetch_october_2025_multiple_cities()

