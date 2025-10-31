"""
Great Expectations validation for raw weather data from S3/MinIO.

This script validates raw JSON files written by the extractor before loading to Postgres.
It checks:
- Required fields exist and are not null
- Temperature bounds are reasonable
- Precipitation is non-negative
- Wind speed is non-negative
- Timestamps are valid

Uses Great Expectations 1.8+ Fluent API.
"""

import os
import json
import boto3
from botocore.config import Config
from typing import Dict, List
import great_expectations as gx
import pandas as pd


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
    """
    Validate weather data using Great Expectations 1.8+ Fluent API.

    Args:
        all_results: Dict with city names as keys, lists of S3 URIs as values

    Returns:
        Validation results dict

    Raises:
        ValueError: if validation fails
    """
    # Fetch and flatten S3 data into records
    print(
        f"Fetching data from {sum(len(v) for v in all_results.values())} S3 objects..."
    )
    records = fetch_s3_objects_as_records(all_results)
    print(f"Fetched {len(records)} hourly records")

    if not records:
        raise ValueError("No records found to validate")

    df = pd.DataFrame(records)
    print(f"DataFrame shape: {df.shape}")

    # Create ephemeral GX context
    context = gx.get_context(mode="ephemeral")

    # Add pandas datasource with dataframe asset
    datasource = context.data_sources.add_pandas("weather_datasource")
    data_asset = datasource.add_dataframe_asset(name="weather_hourly")

    # Add batch definition with dataframe
    batch_definition = data_asset.add_batch_definition_whole_dataframe("weather_batch")

    # Create expectation suite
    suite_name = "openmeteo_raw_weather_suite"
    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))

    # Add expectations to suite
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="time"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="city"))
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="latitude")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="longitude")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="timezone")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="temperature_2m", min_value=-90.0, max_value=60.0, mostly=1.0
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="precipitation", min_value=0.0, max_value=1000.0, mostly=1.0
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="wind_speed_10m", min_value=0.0, max_value=200.0, mostly=1.0
        )
    )

    # Create validation definition with batch definition
    validation_definition = gx.ValidationDefinition(
        data=batch_definition, suite=suite, name="weather_validation"
    )
    validation_definition = context.validation_definitions.add(validation_definition)

    # Run validation with dataframe
    results = validation_definition.run(batch_parameters={"dataframe": df})

    # Process and display results
    _print_validation_results(results)

    # Check if validation passed
    if not results.success:
        failed_count = sum(1 for r in results.results if not r.success)
        total_count = len(results.results)
        raise ValueError(
            f"Data validation failed! {failed_count} of {total_count} expectations failed."
        )

    return results


def _print_validation_results(results) -> None:
    """
    Print formatted validation results.

    Args:
        results: GX validation results object
    """
    print("\n" + "=" * 70)
    print("VALIDATION RESULTS")
    print("=" * 70)
    print(f"Success: {results.success}")

    # Count expectations
    total_expectations = len(results.results)
    successful_expectations = sum(1 for r in results.results if r.success)
    failed_expectations = total_expectations - successful_expectations
    success_percent = (
        (successful_expectations / total_expectations * 100)
        if total_expectations > 0
        else 0
    )

    print(f"\nTotal expectations: {total_expectations}")
    print(f"Successful: {successful_expectations}")
    print(f"Failed: {failed_expectations}")
    print(f"Success percentage: {success_percent:.2f}%")

    if not results.success:
        print("\n" + "=" * 70)
        print("FAILED EXPECTATIONS:")
        print("=" * 70)
        for result in results.results:
            if not result.success:
                expectation_type = result.expectation_config.type
                column = result.expectation_config.kwargs.get("column", "N/A")
                print(f"\n❌ {expectation_type}")
                print(f"   Column: {column}")
                print(f"   Result: {result.result}")


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
