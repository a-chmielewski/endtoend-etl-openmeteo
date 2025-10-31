# dags/backfill_openmeteo.py
from airflow import DAG
from airflow.decorators import task
from airflow.utils import timezone
from datetime import timedelta

DEFAULT_ARGS = dict(retries=2, retry_delay=timedelta(minutes=5))

with DAG(
    dag_id="backfill_openmeteo",
    start_date=timezone.datetime(2025, 10, 30),
    schedule="0 2 * * 0",  # weekly on Sunday at 2 AM
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["openmeteo", "backfill"],
) as dag:

    @task
    def identify_gaps():
        """
        Query Postgres to find missing hours in the last 7 days.
        Returns: dict with city -> list of missing datetime objects
        """
        import datetime as dt
        import psycopg2
        import os

        CITIES = ["Warsaw", "Berlin", "Paris", "London"]

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB", "weather_db"),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        )

        end = dt.datetime.now(dt.UTC).replace(minute=0, second=0, microsecond=0)
        start = end - dt.timedelta(days=7)

        gaps = {}

        with conn.cursor() as cur:
            for city in CITIES:
                # Get all hours that should exist
                expected_hours = []
                current = start
                while current < end:
                    expected_hours.append(current)
                    current += dt.timedelta(hours=1)

                # Get existing hours from database
                cur.execute(
                    """
                    SELECT DISTINCT DATE_TRUNC('hour', timestamp_utc) as hour
                    FROM staging.weather_hourly
                    WHERE city = %s
                    AND timestamp_utc >= %s
                    AND timestamp_utc < %s
                    ORDER BY hour
                    """,
                    (city, start, end),
                )

                existing_hours = {
                    row[0].replace(tzinfo=dt.timezone.utc) for row in cur.fetchall()
                }

                missing_hours = [h for h in expected_hours if h not in existing_hours]

                if missing_hours:
                    gaps[city] = missing_hours
                    print(f"{city}: {len(missing_hours)} missing hours")
                else:
                    print(f"{city}: No gaps found")

        conn.close()

        total_gaps = sum(len(v) for v in gaps.values())
        print(f"\nTotal gaps to backfill: {total_gaps}")

        return gaps

    @task
    def extract_missing(gaps: dict):
        """
        Fetch missing hours from Open-Meteo API and write to S3.
        Returns: dict with keys per city
        """
        import datetime as dt
        from ingestion.extractor.openmeteo_client import fetch_hourly_data
        from ingestion.extractor.s3_writer import write_raw

        CITY_COORDS = {
            "Warsaw": (52.23, 21.01),
            "Berlin": (52.52, 13.41),
            "Paris": (48.86, 2.35),
            "London": (51.51, -0.13),
        }

        if not gaps:
            print("No gaps to backfill")
            return {}

        all_results = {}

        for city, missing_hours in gaps.items():
            print(f"\n--- Backfilling {city}: {len(missing_hours)} hours ---")

            latitude, longitude = CITY_COORDS[city]
            keys = []

            # Group consecutive hours to minimize API calls
            if not missing_hours:
                continue

            missing_hours_sorted = sorted(missing_hours)

            # Fetch in batches (API supports up to ~168 hours)
            batch_size = 24
            for i in range(0, len(missing_hours_sorted), batch_size):
                batch = missing_hours_sorted[i : i + batch_size]
                batch_start = batch[0]
                batch_end = batch[-1] + dt.timedelta(hours=1)

                print(f"  Fetching batch: {batch_start} to {batch_end}")

                payload = fetch_hourly_data(
                    latitude, longitude, batch_start.isoformat(), batch_end.isoformat()
                )

                hourly = payload.get("hourly", {})
                times = hourly.get("time", [])
                temps = hourly.get("temperature_2m", [])
                precips = hourly.get("precipitation", [])
                winds = hourly.get("wind_speed_10m", [])

                if not times:
                    print(f"  ⚠ No data returned for batch")
                    continue

                # Write each hour as separate file
                for idx, time_str in enumerate(times):
                    hour_dt = dt.datetime.fromisoformat(time_str.replace("Z", "+00:00"))

                    if hour_dt.tzinfo is None:
                        hour_dt = hour_dt.replace(tzinfo=dt.timezone.utc)

                    # Only write if this hour was in our missing list
                    if hour_dt not in batch:
                        continue

                    single_hour_payload = {
                        "latitude": payload.get("latitude"),
                        "longitude": payload.get("longitude"),
                        "timezone": payload.get("timezone"),
                        "hourly": {
                            "time": [time_str],
                            "temperature_2m": (
                                [temps[idx]] if idx < len(temps) else [None]
                            ),
                            "precipitation": (
                                [precips[idx]] if idx < len(precips) else [None]
                            ),
                            "wind_speed_10m": (
                                [winds[idx]] if idx < len(winds) else [None]
                            ),
                        },
                    }

                    partition_dt = hour_dt.replace(tzinfo=None)
                    key = write_raw(
                        "raw",
                        "weather",
                        single_hour_payload,
                        city=city,
                        partition_dt=partition_dt,
                    )
                    s3_uri = f"s3://raw/{key}"
                    keys.append(s3_uri)

            print(f"  ✓ Backfilled {len(keys)} files for {city}")
            all_results[city] = keys

        total_keys = sum(len(v) for v in all_results.values())
        print(f"\n=== BACKFILL EXTRACT COMPLETE ===")
        print(f"Total files: {total_keys}")

        return all_results

    @task
    def validate(all_results: dict):
        """Validate backfilled data using Great Expectations."""
        from ge.validate_raw_weather import validate_weather_data

        if not all_results:
            print("No data to validate")
            return all_results

        print("=" * 70)
        print("GREAT EXPECTATIONS VALIDATION (BACKFILL)")
        print("=" * 70)

        total_files = sum(len(v) for v in all_results.values())
        print(f"\nValidating {total_files} backfilled files")

        try:
            validate_weather_data(all_results)
            print("\n✅ All backfill data quality checks passed!")
            return all_results
        except Exception as e:
            print(f"\n❌ BACKFILL VALIDATION FAILED: {e}")
            raise

    @task
    def load(all_results: dict):
        """Load backfilled data into Postgres."""
        from ingestion.loader.load_to_postgres import load_one

        if not all_results:
            print("No data to load")
            return 0

        total_rows = 0

        print(f"=== LOADING BACKFILL DATA ===\n")

        for city, keys in all_results.items():
            print(f"--- Loading {city}: {len(keys)} files ---")
            city_rows = 0

            for s3_uri in keys:
                rows = load_one(s3_uri, city)
                print(f"  ✓ Loaded {s3_uri}: {rows} rows")
                city_rows += rows or 0

            print(f"Total rows for {city}: {city_rows}\n")
            total_rows += city_rows

        print("=== BACKFILL LOAD COMPLETE ===")
        print(f"Total rows loaded: {total_rows}")
        return total_rows

    # DAG flow
    gaps = identify_gaps()
    extracted = extract_missing(gaps)
    validated = validate(extracted)
    load(validated)
