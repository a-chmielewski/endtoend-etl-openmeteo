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


def create_expectation_suite(context):
    """
    Create or update the expectation suite for raw weather data.

    Expectations:
    - time field must not be null
    - temperature_2m must be between -90 and 60 Celsius (extreme but realistic bounds)
    - precipitation must be >= 0 (cannot be negative)
    - wind_speed_10m must be >= 0 (cannot be negative)
    - city, latitude, longitude must not be null
    """
    suite_name = "openmeteo_raw_suite"

    try:
        # Try to get existing suite
        suite = context.get_expectation_suite(expectation_suite_name=suite_name)
        print(f"Using existing expectation suite: {suite_name}")
    except Exception:
        # Create new suite
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)
        print(f"Created new expectation suite: {suite_name}")

    return suite


def validate_weather_data(all_results: Dict[str, List[str]]) -> dict:
    """
    Validate raw weather data from S3 using Great Expectations.

    Args:
        all_results: Dict with city names as keys, lists of S3 URIs as values

    Returns:
        Validation results dictionary

    Raises:
        Exception if validation fails
    """
    # Initialize GE context
    context = gx.get_context(mode="ephemeral")

    # Fetch and flatten S3 data
    print(
        f"Fetching data from {sum(len(v) for v in all_results.values())} S3 objects..."
    )
    records = fetch_s3_objects_as_records(all_results)
    print(f"Fetched {len(records)} hourly records")

    if not records:
        raise ValueError("No records found to validate")

    # Create or get expectation suite
    suite = create_expectation_suite(context)

    # Create a pandas datasource
    datasource = context.sources.add_or_update_pandas(name="pandas_source")
    data_asset = datasource.add_dataframe_asset(name="weather_data")

    # Create batch request
    batch_request = data_asset.build_batch_request(dataframe=records)

    # Get validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite.expectation_suite_name,
    )

    # Define expectations
    print("\nDefining expectations...")

    # Critical fields must not be null
    validator.expect_column_values_to_not_be_null(column="time")
    validator.expect_column_values_to_not_be_null(column="city")
    validator.expect_column_values_to_not_be_null(column="latitude")
    validator.expect_column_values_to_not_be_null(column="longitude")

    # Temperature bounds (reasonable Earth temperature range in Celsius)
    validator.expect_column_values_to_be_between(
        column="temperature_2m",
        min_value=-90.0,
        max_value=60.0,
        mostly=1.0,  # 100% of values
    )

    # Precipitation must be non-negative
    validator.expect_column_values_to_be_between(
        column="precipitation",
        min_value=0.0,
        max_value=1000.0,  # 1000mm is extreme but possible
        mostly=1.0,
    )

    # Wind speed must be non-negative
    validator.expect_column_values_to_be_between(
        column="wind_speed_10m",
        min_value=0.0,
        max_value=200.0,  # ~400 km/h max recorded wind speed
        mostly=1.0,
    )

    # Timezone should not be null
    validator.expect_column_values_to_not_be_null(column="timezone")

    # Save expectations
    validator.save_expectation_suite(discard_failed_expectations=False)

    # Create and run checkpoint
    checkpoint_config = {
        "name": "openmeteo_raw_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "validations": [
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite.expectation_suite_name,
            }
        ],
    }

    checkpoint = context.add_or_update_checkpoint(**checkpoint_config)

    # Run validation
    print("\nRunning validation checkpoint...")
    results = checkpoint.run()

    # Print results
    print("\n" + "=" * 70)
    print("VALIDATION RESULTS")
    print("=" * 70)
    print(f"Success: {results['success']}")
    print(f"Statistics: {results.statistics}")

    validation_results = results.list_validation_results()[0]
    print(
        f"\nTotal expectations: {validation_results['statistics']['evaluated_expectations']}"
    )
    print(f"Successful: {validation_results['statistics']['successful_expectations']}")
    print(f"Failed: {validation_results['statistics']['unsuccessful_expectations']}")
    print(
        f"Success percentage: {validation_results['statistics']['success_percent']:.2f}%"
    )

    # Show failed expectations
    if not results["success"]:
        print("\n" + "=" * 70)
        print("FAILED EXPECTATIONS:")
        print("=" * 70)
        for result in validation_results["results"]:
            if not result["success"]:
                print(f"\n❌ {result['expectation_config']['expectation_type']}")
                print(
                    f"   Column: {result['expectation_config']['kwargs'].get('column', 'N/A')}"
                )
                print(f"   Details: {result.get('result', {})}")

    print("=" * 70 + "\n")

    # Raise exception if validation failed
    if not results["success"]:
        raise ValueError(
            f"Data validation failed! "
            f"{validation_results['statistics']['unsuccessful_expectations']} "
            f"out of {validation_results['statistics']['evaluated_expectations']} "
            f"expectations failed."
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
