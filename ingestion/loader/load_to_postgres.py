# load_to_postgres.py
import os
import json
import psycopg2
import boto3
from botocore.config import Config
from urllib.parse import urlparse
from typing import Iterator, Tuple
from psycopg2.extras import execute_values


def _resolve_endpoint() -> str:
    ep = (
        os.getenv("MINIO_ENDPOINT_URL")
        or os.getenv("MINIO_ENDPOINT")
        or "http://localhost:9000"
    )
    if not ep.startswith("http"):
        ep = f"http://{ep}"
    return ep


S3 = boto3.client(
    "s3",
    endpoint_url=_resolve_endpoint(),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    region_name=os.getenv("MINIO_REGION", "us-east-1"),
    config=Config(s3={"addressing_style": "path"}),
)


def _connect_pg():
    params = dict(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "analytics"),
        user=os.getenv("POSTGRES_USER", "analytics"),
        password=os.getenv("POSTGRES_PASSWORD", "chhHan!hhSsi9o35"),
        connect_timeout=5,
        options="-c statement_timeout=60000",
    )
    try:
        print(
            f"[pg] connecting host={params['host']} db={params['dbname']} user={params['user']} password={params['password']}"
        )
        return psycopg2.connect(**params)
    except psycopg2.OperationalError:
        # If env / IDE forced 'analytics-db' or 'localhost', fall back to IPv4 explicitly
        params["host"] = "localhost"
        print("[pg] retrying with host=localhost")
        return psycopg2.connect(**params)


def iter_s3_keys(bucket: str, prefix: str) -> Iterator[Tuple[str, str]]:
    """
    Yield (key, etag) for all objects under bucket/prefix, paginated.
    """
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = S3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            etag = (obj.get("ETag") or "").strip('"')
            yield obj["Key"], etag
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")


def _load_key_into_db(pg, cur, bucket: str, key: str, city: str) -> int:
    obj = S3.get_object(Bucket=bucket, Key=key)
    payload = json.loads(obj["Body"].read())

    hourly = payload.get("hourly") or {}
    hours = hourly.get("time") or []
    temp = hourly.get("temperature_2m") or []
    precip = hourly.get("precipitation") or []
    wind = hourly.get("wind_speed_10m") or []

    # keep only fully-paired rows
    n = min(len(hours), len(temp), len(precip), len(wind))
    if n == 0:
        return 0
    rows = [(city, hours[i], temp[i], precip[i], wind[i]) for i in range(n)]

    # Upsert in one roundtrip (faster than many INSERTs)
    execute_values(
        cur,
        """
        INSERT INTO staging.weather_hourly
            (city, "timestamp", temperature_2m, precipitation, wind_speed_10m)
        VALUES %s
        ON CONFLICT (city, "timestamp") DO UPDATE
        SET temperature_2m = EXCLUDED.temperature_2m,
            precipitation  = EXCLUDED.precipitation,
            wind_speed_10m = EXCLUDED.wind_speed_10m
        """,
        rows,
    )
    return n


def load_one(s3_uri: str, city: str) -> int:
    # s3://raw/weather/ds=.../openmeteo_...json
    parsed = urlparse(s3_uri)
    obj = S3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
    payload = json.loads(obj["Body"].read())

    hours = payload["hourly"]["time"]
    temp = payload["hourly"]["temperature_2m"]
    precip = payload["hourly"]["precipitation"]
    wind = payload["hourly"]["wind_speed_10m"]
    rows = list(zip(hours, temp, precip, wind))

    with _connect_pg() as pg, pg.cursor() as cur:
        for ts, t, p, w in rows:
            cur.execute(
                """
                INSERT INTO staging.weather_hourly
                    (city, "timestamp", temperature_2m, precipitation, wind_speed_10m)
                VALUES
                    (%s, %s::timestamptz, %s, %s, %s)
                ON CONFLICT (city, "timestamp") DO UPDATE
                SET temperature_2m = EXCLUDED.temperature_2m,
                    precipitation  = EXCLUDED.precipitation,
                    wind_speed_10m = EXCLUDED.wind_speed_10m;
                """,
                (city, ts, t, p, w),
            )
    return len(rows)


def load_all_weather(
    city: str,
    bucket: str = "raw",
    prefix: str = "weather/",
    skip_logged: bool = True,
    limit_files: int | None = None,
) -> tuple[int, int]:
    """
    Load ALL objects under s3://{bucket}/{prefix} into staging.weather_hourly.
    Returns (files_processed, total_rows_inserted).
    If skip_logged=True, records processed keys and skips them next time.
    """
    files = 0
    total_rows = 0
    with _connect_pg() as pg, pg.cursor() as cur:
        if skip_logged:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS staging._ingest_log (
                    bucket text NOT NULL,
                    key    text PRIMARY KEY,
                    etag   text,
                    rows_inserted int,
                    ingested_at timestamptz DEFAULT now()
                )
            """
            )

        for key, etag in iter_s3_keys(bucket, prefix):
            if skip_logged:
                cur.execute("SELECT 1 FROM staging._ingest_log WHERE key = %s", (key,))
                if cur.fetchone():
                    continue  # already processed

            rows = _load_key_into_db(pg, cur, bucket, key, city)
            total_rows += rows
            files += 1

            if skip_logged:
                cur.execute(
                    """
                    INSERT INTO staging._ingest_log (bucket, key, etag, rows_inserted)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (key) DO UPDATE
                      SET etag = EXCLUDED.etag,
                          rows_inserted = EXCLUDED.rows_inserted,
                          ingested_at = now()
                """,
                    (bucket, key, etag, rows),
                )

            if limit_files and files >= limit_files:
                break

    return files, total_rows
