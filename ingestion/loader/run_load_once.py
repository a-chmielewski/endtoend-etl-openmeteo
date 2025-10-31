import os
import boto3
from botocore.config import Config
from load_to_postgres import load_all_weather


def _resolve_endpoint() -> str:
    ep = (
        os.getenv("MINIO_ENDPOINT_URL")
        or os.getenv("MINIO_ENDPOINT")
        or "http://localhost:9000"
    )
    if not ep.startswith("http"):
        ep = f"http://{ep}"
    return ep


def _to_bool(s: str | None, default=True) -> bool:
    if s is None:
        return default
    return s.strip().lower() not in {"0", "false", "no", "off"}


def _to_int(s: str | None):
    try:
        return int(s) if s is not None else None
    except ValueError:
        return None


def discover_cities(bucket: str, base_prefix: str = "weather/") -> list[str]:
    """Discover city folders in MinIO by listing prefixes under base_prefix."""
    s3 = boto3.client(
        "s3",
        endpoint_url=_resolve_endpoint(),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )

    cities = set()
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=base_prefix, Delimiter="/"):
        # CommonPrefixes gives us the "folders"
        for prefix_info in page.get("CommonPrefixes", []):
            prefix = prefix_info["Prefix"]
            # Extract city name: "weather/Warsaw/" -> "Warsaw"
            city = prefix.replace(base_prefix, "").strip("/")
            if city:
                cities.add(city)

    return sorted(cities)


if __name__ == "__main__":
    bucket = os.getenv("S3_BUCKET", "raw")
    base_prefix = os.getenv("BASE_PREFIX", "weather/")
    skip_logged = _to_bool(os.getenv("SKIP_LOGGED"), default=True)
    limit_files = _to_int(
        os.getenv("LIMIT_FILES")
    )  # optional: cap number of files this run

    print("=" * 60)
    print("Discovering cities from MinIO...")
    print("=" * 60)

    cities = discover_cities(bucket, base_prefix)

    if not cities:
        print(f"No cities found in s3://{bucket}/{base_prefix}")
        print("Make sure you've run the extraction script first.")
        exit(0)

    print(f"Found {len(cities)} cities: {', '.join(cities)}\n")

    total_files = 0
    total_rows = 0

    for city in cities:
        prefix = f"{base_prefix}{city}/"
        print(f"[{city}] Loading from s3://{bucket}/{prefix}")

        try:
            files, rows = load_all_weather(
                city=city,
                bucket=bucket,
                prefix=prefix,
                skip_logged=skip_logged,
                limit_files=limit_files,
            )
            print(f"[{city}] ✓ Processed {files} files, {rows} rows upserted")
            total_files += files
            total_rows += rows
        except Exception as e:
            print(f"[{city}] ✗ Failed: {e}")

    print("\n" + "=" * 60)
    print(
        f"Total: {total_files} files, {total_rows} rows upserted across {len(cities)} cities"
    )
    print("=" * 60)
