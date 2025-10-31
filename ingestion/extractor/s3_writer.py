import json
import io
import datetime as dt
import boto3
import os
from botocore.config import Config

def _resolve_endpoint() -> str:
    # Prefer a full URL if provided, else a host:port, else default
    ep = os.getenv("MINIO_ENDPOINT_URL") or os.getenv("MINIO_ENDPOINT") or "http://localhost:9000"
    if not ep.startswith("http"):
        ep = f"http://{ep}"
    return ep

ENDPOINT = _resolve_endpoint()
# print(f"[s3_writer] Using S3 endpoint: {ENDPOINT}")

S3 = boto3.client("s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    region_name=os.getenv("MINIO_REGION","us-east-1"),
    config=Config(s3={"addressing_style": "path"})
)

def write_raw(bucket: str, prefix: str, payload: dict, city: str = None, partition_dt: dt.datetime = None) -> str:
    """
    Write raw JSON payload to S3/MinIO.
    
    Args:
        bucket: S3 bucket name
        prefix: Key prefix (e.g., 'weather')
        payload: JSON payload to write
        city: City name (creates city subfolder if provided)
        partition_dt: Date to use for partitioning (defaults to now for backwards compatibility)
    
    Returns:
        S3 key of written object
    """
    # Use provided partition date or default to now
    partition_date = partition_dt or dt.datetime.utcnow()
    ingest_ts = dt.datetime.utcnow()
    
    if city:
        key = f"{prefix}/{city}/ds={partition_date:%Y-%m-%d}/hour={partition_date:%H}/openmeteo_{ingest_ts:%Y%m%dT%H%M%S}.json"
    else:
        key = f"{prefix}/ds={partition_date:%Y-%m-%d}/hour={partition_date:%H}/openmeteo_{ingest_ts:%Y%m%dT%H%M%S}.json"
    
    data = json.dumps(payload).encode()
    S3.put_object(Bucket=bucket, Key=key, Body=io.BytesIO(data), ContentType="application/json")
    return key
