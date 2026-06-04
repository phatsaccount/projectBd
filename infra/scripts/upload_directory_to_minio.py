import argparse
import os
from pathlib import Path

import boto3
from botocore.client import Config


def parse_args():
    parser = argparse.ArgumentParser(description="Upload a local directory to MinIO.")
    parser.add_argument("--local-path", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "moviemaster"))
    parser.add_argument("--clear-prefix", action="store_true")
    return parser.parse_args()


def create_client():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    secure = os.getenv("MINIO_SECURE", "false").strip().lower() in {"1", "true", "yes"}

    if secure and endpoint.startswith("http://"):
        endpoint = "https://" + endpoint[len("http://") :]

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket(client, bucket):
    buckets = client.list_buckets().get("Buckets", [])
    if any(item.get("Name") == bucket for item in buckets):
        return
    client.create_bucket(Bucket=bucket)


def clear_prefix(client, bucket, prefix):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=bucket, Delete={"Objects": objects})


def upload_directory(client, bucket, local_path, prefix):
    local_root = Path(local_path)
    if not local_root.exists():
        raise FileNotFoundError(f"Local path does not exist: {local_root}")

    uploaded = 0
    for file_path in local_root.rglob("*"):
        if not file_path.is_file():
            continue
        relative = file_path.relative_to(local_root).as_posix()
        key = f"{prefix.strip('/')}/{relative}"
        client.upload_file(str(file_path), bucket, key)
        uploaded += 1

    print(f"Uploaded {uploaded} files from {local_root} to s3://{bucket}/{prefix.strip('/')}")


def main():
    args = parse_args()
    client = create_client()
    ensure_bucket(client, args.bucket)
    if args.clear_prefix:
        clear_prefix(client, args.bucket, args.prefix)
    upload_directory(client, args.bucket, args.local_path, args.prefix)


if __name__ == "__main__":
    main()
