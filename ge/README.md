# Great Expectations Data Validation

This directory contains Great Expectations configuration and validation scripts for the OpenMeteo ETL pipeline.

## Overview

The validation layer sits between the **Extract** and **Load** phases of the ETL pipeline:

```
Extract → Validate (GE) → Load → Transform (dbt)
```

If validation fails, the Load task is blocked, preventing bad data from entering the database.

## Validation Rules

The `openmeteo_raw_suite` expectation suite validates:

### Critical Fields (Not Null)
- `time` - hourly timestamp must exist
- `city` - city name must exist  
- `latitude` - location latitude must exist
- `longitude` - location longitude must exist
- `timezone` - timezone must exist

### Data Quality Bounds
- `temperature_2m` - must be between -90°C and 60°C
- `precipitation` - must be between 0mm and 1000mm
- `wind_speed_10m` - must be between 0 m/s and 200 m/s

## Files

- **`validate_raw_weather.py`** - Main validation logic
  - Fetches raw JSON files from S3/MinIO
  - Creates expectation suite
  - Runs validation checkpoint
  - Returns results or raises exception on failure

- **`run_checkpoint.py`** - CLI wrapper for manual testing

- **`__init__.py`** - Package initialization

## Usage

The validation is automatically run by the `validate` task in the Airflow DAG:
