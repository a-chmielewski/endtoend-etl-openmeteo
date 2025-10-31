"""
Great Expectations validation for raw weather data from S3/MinIO.

This script validates raw JSON files written by the extractor before loading to Postgres.
It checks:
- Required fields exist and are not null
- Temperature bounds are reasonable
- Precipitation is non-negative
- Wind speed is non-negative
- Timestamps are valid
"""

import os
import json
import boto3
from botocore.config import Config
from typing import Dict, List
import great_expectations as gx


def _resolve_endpoint() -> str:
    """Resolve MinIO endpoint from environment variables."""
    ep = (
        os.getenv("MINIO_ENDPOINT_URL")
        or os.getenv("MINIO_ENDPOINT")
        or "http://localhost:9000"
    )
    if not ep.startswith("http"):
        ep = f"http://{ep}"
    return ep


def _get_s3_client():
    """Create boto3 S3 client for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=_resolve_endpoint(),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )


def fetch_s3_objects_as_records(all_results: Dict[str, List[str]]) -> List[dict]:
    """
    Fetch all S3 objects referenced in all_results and convert to flat records.

    Args:
        all_results: Dict with city names as keys, lists of S3 URIs as values

    Returns:
        List of flattened records, one per hourly data point
    """
    s3_client = _get_s3_client()
    records = []

    for city, s3_uris in all_results.items():
        for s3_uri in s3_uris:
            # Parse s3://bucket/key
            parts = s3_uri.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key = parts[1]

            # Fetch object
            try:
                obj = s3_client.get_object(Bucket=bucket, Key=key)
                payload = json.loads(obj["Body"].read())

                # Extract hourly data
                hourly = payload.get("hourly", {})
                times = hourly.get("time", [])
                temps = hourly.get("temperature_2m", [])
                precips = hourly.get("precipitation", [])
                winds = hourly.get("wind_speed_10m", [])

                # Create one record per hourly data point
                for i in range(len(times)):
                    record = {
                        "city": city,
                        "s3_uri": s3_uri,
                        "latitude": payload.get("latitude"),
                        "longitude": payload.get("longitude"),
                        "timezone": payload.get("timezone"),
                        "time": times[i] if i < len(times) else None,
                        "temperature_2m": temps[i] if i < len(temps) else None,
                        "precipitation": precips[i] if i < len(precips) else None,
                        "wind_speed_10m": winds[i] if i < len(winds) else None,
                    }
                    records.append(record)

            except Exception as e:
                print(f"Error fetching {s3_uri}: {e}")
                raise

    return records


def validate_weather_data(all_results: Dict[str, List[str]]) -> dict:
    import pandas as pd
    import great_expectations as gx
    from great_expectations.core.expectation_suite import ExpectationSuite

    # Fetch and flatten
    print(f"Fetching data from {sum(len(v) for v in all_results.values())} S3 objects...")
    records = fetch_s3_objects_as_records(all_results)
    print(f"Fetched {len(records)} hourly records")
    if not records:
        raise ValueError("No records found to validate")

    df = pd.DataFrame(records)

    # ---- Fluent API start ----
    # fluent file-based context (works with `.sources`)
    context = gx.get_context()

    # add a Pandas datasource & a DataFrame asset (Fluent)
    ds = context.sources.add_pandas(name="pandas_src")
    asset = ds.add_dataframe_asset(name="weather_data")

    # build batch request from the in-memory DataFrame
    batch_request = asset.build_batch_request(dataframe=df)

    suite_name = "openmeteo_raw_suite"
    # make sure the suite exists (idempotent)
    try:
        context.suites.add(ExpectationSuite(name=suite_name))
    except Exception:
        # If it already exists, ignore
        pass

    # get validator against that suite
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )
    # ---- Fluent API end ----

    print("\nDefining expectations...")
    validator.expect_column_values_to_not_be_null(column="time")
    validator.expect_column_values_to_not_be_null(column="city")
    validator.expect_column_values_to_not_be_null(column="latitude")
    validator.expect_column_values_to_not_be_null(column="longitude")

    validator.expect_column_values_to_be_between(
        column="temperature_2m", min_value=-90.0, max_value=60.0, mostly=1.0
    )
    validator.expect_column_values_to_be_between(
        column="precipitation", min_value=0.0, max_value=1000.0, mostly=1.0
    )
    validator.expect_column_values_to_be_between(
        column="wind_speed_10m", min_value=0.0, max_value=200.0, mostly=1.0
    )
    validator.expect_column_values_to_not_be_null(column="timezone")

    validator.save_expectation_suite(discard_failed_expectations=False)

    print("\nRunning validation...")
    results = validator.validate()

    print("\n" + "=" * 70)
    print("VALIDATION RESULTS")
    print("=" * 70)
    print(f"Success: {results['success']}")
    stats = results.get("statistics", {})
    print(f"\nTotal expectations: {stats.get('evaluated_expectations', 0)}")
    print(f"Successful: {stats.get('successful_expectations', 0)}")
    print(f"Failed: {stats.get('unsuccessful_expectations', 0)}")
    print(f"Success percentage: {stats.get('success_percent', 0):.2f}%")

    if not results["success"]:
        print("\n" + "=" * 70)
        print("FAILED EXPECTATIONS:")
        print("=" * 70)
        for r in results.get("results", []):
            if not r.get("success", True):
                exp = r.get("expectation_config", {})
                print(f"\n❌ {exp.get('expectation_type', 'Unknown')}")
                print(f"   Column: {exp.get('kwargs', {}).get('column', 'N/A')}")
                print(f"   Details: {r.get('result', {})}")
        raise ValueError(
            f"Data validation failed! "
            f"{stats.get('unsuccessful_expectations', 0)} of "
            f"{stats.get('evaluated_expectations', 0)} expectations failed."
        )

    return results



if __name__ == "__main__":
    # Test with sample data structure
    sample_results = {
        "Warsaw": [
            "s3://raw/weather/Warsaw/ds=2025-10-31/hour=12/openmeteo_20251031T120000.json"
        ]
    }

    try:
        results = validate_weather_data(sample_results)
        print("\n✅ Validation passed!")
    except Exception as e:
        print(f"\n❌ Validation failed: {e}")
        raise
