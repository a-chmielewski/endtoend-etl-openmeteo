"""
Great Expectations validation module for OpenMeteo ETL pipeline.

This module provides data quality validation for raw weather data
before it's loaded into the Postgres database.
"""

from .validate_raw_weather import validate_weather_data

__all__ = ["validate_weather_data"]
