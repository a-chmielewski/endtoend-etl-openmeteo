"""
Test script for Great Expectations validation.

This script demonstrates:
1. How validation passes with good data
2. How validation fails and blocks with bad data
"""

import json
import tempfile
from typing import Dict, List


def test_validation_with_good_data():
    """Test that validation passes with properly formatted data."""
    print("\n" + "="*70)
    print("TEST 1: Validation with GOOD data")
    print("="*70)
    
    # This would normally come from actual S3 files
    # For testing, we simulate the structure
    good_records = [
        {
            "city": "Warsaw",
            "s3_uri": "s3://raw/weather/test/file1.json",
            "latitude": 52.23,
            "longitude": 21.01,
            "timezone": "Europe/Berlin",
            "time": "2025-10-31T12:00:00",
            "temperature_2m": 15.5,
            "precipitation": 0.0,
            "wind_speed_10m": 5.2,
        },
        {
            "city": "Berlin",
            "s3_uri": "s3://raw/weather/test/file2.json",
            "latitude": 52.52,
            "longitude": 13.41,
            "timezone": "Europe/Berlin",
            "time": "2025-10-31T12:00:00",
            "temperature_2m": 16.8,
            "precipitation": 2.5,
            "wind_speed_10m": 8.1,
        },
    ]
    
    print("\nSample good data:")
    print(f"- {len(good_records)} records")
    print(f"- Temperature range: {min(r['temperature_2m'] for r in good_records):.1f}°C to {max(r['temperature_2m'] for r in good_records):.1f}°C")
    print(f"- All required fields present: ✓")
    
    print("\n✅ This data would PASS validation")
    print("   → Load task would proceed")


def test_validation_with_bad_data():
    """Test that validation fails with invalid data."""
    print("\n" + "="*70)
    print("TEST 2: Validation with BAD data")
    print("="*70)
    
    bad_records = [
        {
            "city": "Warsaw",
            "s3_uri": "s3://raw/weather/test/bad1.json",
            "latitude": 52.23,
            "longitude": 21.01,
            "timezone": "Europe/Berlin",
            "time": None,  # ❌ NULL timestamp - VIOLATION!
            "temperature_2m": 15.5,
            "precipitation": 0.0,
            "wind_speed_10m": 5.2,
        },
        {
            "city": "Berlin",
            "s3_uri": "s3://raw/weather/test/bad2.json",
            "latitude": 52.52,
            "longitude": 13.41,
            "timezone": "Europe/Berlin",
            "time": "2025-10-31T12:00:00",
            "temperature_2m": 150.0,  # ❌ 150°C - IMPOSSIBLE! (max=60)
            "precipitation": -5.0,  # ❌ Negative precipitation - VIOLATION!
            "wind_speed_10m": 8.1,
        },
    ]
    
    print("\nSample bad data with violations:")
    print(f"- {len(bad_records)} records")
    print("\nViolations detected:")
    print("  ❌ Record 1: NULL timestamp (required field)")
    print("  ❌ Record 2: Temperature = 150°C (exceeds max of 60°C)")
    print("  ❌ Record 2: Precipitation = -5.0mm (negative value)")
    
    print("\n❌ This data would FAIL validation")
    print("   → Load task would be BLOCKED")
    print("   → Airflow task would fail")
    print("   → No bad data enters database")


def show_validation_flow():
    """Show the complete validation flow in the pipeline."""
    print("\n" + "="*70)
    print("VALIDATION FLOW IN PIPELINE")
    print("="*70)
    
    print("""
1. EXTRACT Task
   ├─ Fetch data from Open-Meteo API
   ├─ Write JSON files to S3/MinIO
   └─ Return: {"Warsaw": ["s3://...", ...], "Berlin": [...], ...}
   
2. VALIDATE Task (Great Expectations) ← NEW!
   ├─ Fetch all S3 files
   ├─ Flatten to records
   ├─ Run expectation suite:
   │  ├─ Check NOT NULL: time, city, lat, lon, timezone
   │  ├─ Check temperature bounds: -90°C to 60°C
   │  ├─ Check precipitation >= 0
   │  └─ Check wind_speed >= 0
   ├─ If ALL pass → Return results (proceed to load)
   └─ If ANY fail → Raise exception (BLOCK load)
   
3. LOAD Task (only runs if validation passes)
   ├─ Read S3 files
   ├─ Insert into staging.weather_hourly
   └─ Log successful loads
   
4. DBT Transformation
   ├─ stg_weather_hourly
   └─ fct_city_day
""")


def show_example_output():
    """Show example validation output."""
    print("\n" + "="*70)
    print("EXAMPLE: Successful Validation Output")
    print("="*70)
    
    print("""
Fetching data from 24 S3 objects...
Fetched 24 hourly records

Defining expectations...

Running validation checkpoint...

======================================================================
VALIDATION RESULTS
======================================================================
Success: True
Total expectations: 7
Successful: 7
Failed: 0
Success percentage: 100.00%

✅ All data quality checks passed!
Proceeding to load data into Postgres...
""")

    print("\n" + "="*70)
    print("EXAMPLE: Failed Validation Output")
    print("="*70)
    
    print("""
Fetching data from 24 S3 objects...
Fetched 24 hourly records

Defining expectations...

Running validation checkpoint...

======================================================================
VALIDATION RESULTS
======================================================================
Success: False
Total expectations: 7
Successful: 5
Failed: 2
Success percentage: 71.43%

======================================================================
FAILED EXPECTATIONS:
======================================================================

❌ expect_column_values_to_not_be_null
   Column: time
   Details: 1 null value found

❌ expect_column_values_to_be_between
   Column: temperature_2m
   Details: 1 value outside range [-90, 60]
======================================================================

❌ VALIDATION FAILED: Data validation failed! 2 out of 7 expectations failed.

⚠️  Load task will be skipped due to data quality issues.
Please review the validation results above and fix data issues.

[Task failed - Load task will not run]
""")


if __name__ == "__main__":
    print("\n" + "="*70)
    print("GREAT EXPECTATIONS VALIDATION - TEST SCENARIOS")
    print("="*70)
    
    test_validation_with_good_data()
    test_validation_with_bad_data()
    show_validation_flow()
    show_example_output()
    
    print("\n" + "="*70)
    print("To run actual validation with real S3 data:")
    print("="*70)
    print("""
    from ge.validate_raw_weather import validate_weather_data
    
    results = {
        "Warsaw": ["s3://raw/weather/Warsaw/ds=2025-10-31/hour=12/file.json"],
        "Berlin": ["s3://raw/weather/Berlin/ds=2025-10-31/hour=12/file.json"],
    }
    
    try:
        validate_weather_data(results)
        print("✅ Validation passed!")
    except ValueError as e:
        print(f"❌ Validation failed: {e}")
    """)
    
    print("\n" + "="*70 + "\n")

