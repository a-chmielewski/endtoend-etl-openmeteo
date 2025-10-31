"""
Command-line script to run Great Expectations checkpoint.

Usage:
    python ge/run_checkpoint.py

This script is called by Airflow after extraction completes.
It validates the raw weather data before loading to Postgres.
"""

import sys


def main():
    """
    Run the checkpoint validation.
    
    Note: In Airflow, we'll pass the extraction results directly through XCom.
    This standalone script is for manual testing.
    """
    print("="*70)
    print("Great Expectations - OpenMeteo Raw Data Validation")
    print("="*70)
    
    # For standalone testing, this would need actual S3 data
    # In production, Airflow will call validate_weather_data() directly with XCom data
    print("\nThis script should be called from Airflow with extraction results.")
    print("For manual testing, modify validate_raw_weather.py's __main__ block.")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

