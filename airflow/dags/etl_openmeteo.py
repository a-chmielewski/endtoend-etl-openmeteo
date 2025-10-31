# dags/etl_openmeteo.py
from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone
from datetime import timedelta

# retries live in default_args for all tasks
DEFAULT_ARGS = dict(retries=1, retry_delay=timedelta(minutes=1))

with DAG(
    dag_id="etl_openmeteo",
    # Use a fixed start_date (recommended). Adjust if needed.
    start_date=timezone.datetime(2025, 10, 30),
    schedule="0 * * * *",  # hourly at :00
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["openmeteo"],
) as dag:

    @task
    def extract():
        """
        Fetch last 6 hours of Open-Meteo data for all cities and write one S3 object per hour per city.
        Returns: dict with keys per city
        """
        import datetime as dt
        from ingestion.extractor.openmeteo_client import fetch_hourly_data
        from ingestion.extractor.s3_writer import write_raw

        CITIES = {
            "Warsaw": (52.23, 21.01),
            "Berlin": (52.52, 13.41),
            "Paris": (48.86, 2.35),
            "London": (51.51, -0.13),
        }

        end = dt.datetime.now(dt.UTC).replace(minute=0, second=0, microsecond=0)
        start = end - dt.timedelta(hours=6)

        print(f"Fetching last 6 hours of data for {len(CITIES)} cities")
        print(f"Time range: {start} to {end}")

        all_results = {}

        for city, (latitude, longitude) in CITIES.items():
            print(f"\n--- Processing {city} ({latitude}, {longitude}) ---")

            payload = fetch_hourly_data(
                latitude, longitude, start.isoformat(), end.isoformat()
            )

            hourly = payload.get("hourly", {})
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            precips = hourly.get("precipitation", [])
            winds = hourly.get("wind_speed_10m", [])

            if not times:
                print(f"⚠ No hourly data returned for {city}")
                continue

            print(f"Got {len(times)} hourly data points from API")

            keys = []
            for i, time_str in enumerate(times):
                hour_dt = dt.datetime.fromisoformat(time_str.replace("Z", "+00:00"))

                # Ensure hour_dt is UTC-aware for comparison
                if hour_dt.tzinfo is None:
                    hour_dt = hour_dt.replace(tzinfo=dt.timezone.utc)

                # Filter: only keep hours within our 6-hour window
                if not (start <= hour_dt < end):
                    continue

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

                # partition by the *actual* hour as naive dt
                partition_dt = hour_dt.replace(tzinfo=None)
                key = write_raw(
                    "raw",
                    "weather",
                    single_hour_payload,
                    city=city,
                    partition_dt=partition_dt,
                )
                s3_uri = f"s3://raw/{key}"
                print(f"  ✓ Wrote to {s3_uri}")
                keys.append(s3_uri)

            print(f"Total files written for {city}: {len(keys)}")
            all_results[city] = keys

        total_keys = sum(len(v) for v in all_results.values())
        print("\n=== EXTRACT COMPLETE ===")
        print(f"Total cities: {len(all_results)}")
        print(f"Total files: {total_keys}")

        return all_results

    @task
    def validate(all_results: dict):
        """
        Validate raw weather data using Great Expectations.
        This task runs AFTER extraction and BEFORE loading.

        Args:
            all_results: dict with city names as keys, lists of S3 URIs as values

        Returns:
            all_results (passed through on success)

        Raises:
            ValueError: if validation fails (blocks the load task)
        """
        from ge.validate_raw_weather import validate_weather_data

        print("=" * 70)
        print("GREAT EXPECTATIONS VALIDATION")
        print("=" * 70)

        total_files = sum(len(v) for v in all_results.values())
        print(f"\nValidating {total_files} files across {len(all_results)} cities")

        try:
            # Run GE validation - raises ValueError if validation fails
            validate_weather_data(all_results)

            print("\n✅ All data quality checks passed!")
            print("Proceeding to load data into Postgres...\n")

            # Return all_results to pass to next task
            return all_results

        except Exception as e:
            print(f"\n❌ VALIDATION FAILED: {e}")
            print("\n⚠️  Load task will be skipped due to data quality issues.")
            print("Please review the validation results above and fix data issues.\n")
            raise  # This will fail the task and block the load

    @task
    def load(all_results: dict):
        """
        Load each written S3 object into Postgres using your loader.
        all_results: dict with city names as keys, lists of S3 keys as values
        """
        from ingestion.loader.load_to_postgres import load_one

        total_rows = 0

        print(f"=== LOADING DATA FOR {len(all_results)} CITIES ===\n")

        for city, keys in all_results.items():
            print(f"--- Loading {city}: {len(keys)} files ---")
            city_rows = 0

            for s3_uri in keys:
                rows = load_one(s3_uri, city)
                print(f"  ✓ Loaded {s3_uri}: {rows} rows")
                city_rows += rows or 0

            print(f"Total rows for {city}: {city_rows}\n")
            total_rows += city_rows

        print("=== LOAD COMPLETE ===")
        print(f"Total rows loaded across all cities: {total_rows}")
        return total_rows

    # DAG flow: extract -> validate -> load
    # The validate task will block load if validation fails
    extracted_data = extract()
    validated_data = validate(extracted_data)
    load(validated_data)
