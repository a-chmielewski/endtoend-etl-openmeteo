import os
from load_to_postgres import load_all_weather

def _to_bool(s: str | None, default=True) -> bool:
    if s is None:
        return default
    return s.strip().lower() not in {"0", "false", "no", "off"}

def _to_int(s: str | None):
    try:
        return int(s) if s is not None else None
    except ValueError:
        return None

if __name__ == "__main__":
    city = os.getenv("WEATHER_CITY", "Warsaw")
    bucket = os.getenv("S3_BUCKET", "raw")
    prefix = os.getenv("S3_PREFIX", "weather/")  # loads everything under weather/
    skip_logged = _to_bool(os.getenv("SKIP_LOGGED"), default=True)
    limit_files = _to_int(os.getenv("LIMIT_FILES"))  # optional: cap number of files this run

    files, rows = load_all_weather(
        city=city,
        bucket=bucket,
        prefix=prefix,
        skip_logged=skip_logged,
        limit_files=limit_files,
    )
    print(f"Processed files: {files}, rows upserted: {rows}")
