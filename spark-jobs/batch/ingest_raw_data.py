import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

SPARK_JOBS_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SPARK_JOBS_DIR.parent
sys.path.append(str(SPARK_JOBS_DIR))

from common.schemas import dataset_names, get_filename, get_schema


def _bool_env(name, default="false"):
    value = os.getenv(name, default).strip().lower()
    return value in {"1", "true", "yes"}


def _join_path(base, tail):
    if base.startswith("s3a://"):
        return base.rstrip("/") + "/" + tail.lstrip("/")
    return os.path.join(base, tail)


def _build_s3a_path(bucket, prefix):
    prefix = prefix.strip("/")
    if prefix:
        return f"s3a://{bucket}/{prefix}"
    return f"s3a://{bucket}"


def _spark_session(app_name):
    builder = SparkSession.builder.appName(app_name)

    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if endpoint and access_key and secret_key:
        secure = _bool_env("MINIO_SECURE", "false")
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(secure).lower())
        )

    return builder.getOrCreate()


def _validate_s3a_env():
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    missing = [
        name
        for name, value in {
            "MINIO_ENDPOINT": endpoint,
            "MINIO_ACCESS_KEY": access_key,
            "MINIO_SECRET_KEY": secret_key,
        }.items()
        if not value
    ]
    if missing:
        raise ValueError(
            "Missing MinIO configuration: " + ", ".join(sorted(missing))
        )


def _resolve_paths():
    raw_bucket = os.getenv("MINIO_BUCKET_RAW", "").strip()
    raw_prefix = os.getenv("MINIO_RAW_PREFIX", "")
    landing_bucket = os.getenv("MINIO_BUCKET_LANDING", "").strip()
    landing_prefix = os.getenv("MINIO_LANDING_PREFIX", "landing")

    if raw_bucket:
        _validate_s3a_env()
        raw_base = _build_s3a_path(raw_bucket, raw_prefix)
        if landing_bucket:
            landing_base = _build_s3a_path(landing_bucket, landing_prefix)
        else:
            landing_base = _build_s3a_path(raw_bucket, landing_prefix)
    else:
        raw_base = os.getenv("RAW_LOCAL_DIR", str(REPO_ROOT / "data" / "raw"))
        landing_base = os.getenv(
            "LANDING_LOCAL_DIR", str(REPO_ROOT / "data" / "landing")
        )
        os.makedirs(landing_base, exist_ok=True)

    return raw_base, landing_base


def _validate_header(spark, source_path, schema):
    header_df = spark.read.option("header", "true").csv(source_path)
    expected = schema.fieldNames()
    if header_df.columns != expected:
        raise ValueError(
            "Header mismatch for "
            f"{source_path}. Expected {expected}, got {header_df.columns}"
        )


def main():
    spark = _spark_session("ingest_raw_data")
    raw_base, landing_base = _resolve_paths()

    for dataset in dataset_names():
        filename = get_filename(dataset)
        schema = get_schema(dataset)
        source_path = _join_path(raw_base, filename)
        target_path = _join_path(landing_base, dataset)

        _validate_header(spark, source_path, schema)
        df = (
            spark.read.option("header", "true")
            .option("mode", "FAILFAST")
            .schema(schema)
            .csv(source_path)
        )

        df.write.mode("overwrite").parquet(target_path)
        print(f"Ingested {dataset} -> {target_path}")

    spark.stop()


if __name__ == "__main__":
    main()
