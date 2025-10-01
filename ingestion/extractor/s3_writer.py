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

def write_raw(bucket: str, prefix: str, payload: dict) -> str:
    now = dt.datetime.utcnow()
    key = f"{prefix}/ds={now:%Y-%m-%d}/hour={now:%H}/openmeteo_{now:%Y%m%dT%H%M%S}.json"
    data = json.dumps(payload).encode()
    S3.put_object(Bucket=bucket, Key=key, Body=io.BytesIO(data), ContentType="application/json")
    return key
