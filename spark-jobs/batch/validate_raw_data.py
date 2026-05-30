import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SPARK_JOBS_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SPARK_JOBS_DIR.parent
sys.path.append(str(SPARK_JOBS_DIR))

from common.schemas import dataset_names


DATASET_KEYS = {
    "movies": ["movieId"],
    "ratings": ["userId", "movieId", "timestamp"],
    "tags": ["userId", "movieId", "tag", "timestamp"],
    "links": ["movieId"],
    "genome_scores": ["movieId", "tagId"],
    "genome_tags": ["tagId"],
}

REQUIRED_COLUMNS = {
    "movies": ["movieId", "title"],
    "ratings": ["userId", "movieId", "rating", "timestamp"],
    "tags": ["userId", "movieId", "tag", "timestamp"],
    "links": ["movieId"],
    "genome_scores": ["movieId", "tagId", "relevance"],
    "genome_tags": ["tagId", "tag"],
}


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
    landing_bucket = os.getenv("MINIO_BUCKET_LANDING", "").strip()
    landing_prefix = os.getenv("MINIO_LANDING_PREFIX", "landing")

    if landing_bucket:
        _validate_s3a_env()
        landing_base = _build_s3a_path(landing_bucket, landing_prefix)
    else:
        landing_base = os.getenv(
            "LANDING_LOCAL_DIR", str(REPO_ROOT / "data" / "landing")
        )
        os.makedirs(landing_base, exist_ok=True)

    report_path = os.getenv("VALIDATION_REPORT_PATH", "")
    if report_path:
        return landing_base, report_path

    report_base = _join_path(landing_base, "_validation")
    report_path = _join_path(report_base, "report")
    return landing_base, report_path


def _null_count(df, column):
    return df.filter(F.col(column).isNull() | (F.col(column) == "")).count()


def main():
    spark = _spark_session("validate_raw_data")
    landing_base, report_path = _resolve_paths()

    records = []
    for dataset in dataset_names():
        dataset_path = _join_path(landing_base, dataset)
        try:
            df = spark.read.parquet(dataset_path)
        except Exception as exc:
            records.append(
                {
                    "dataset": dataset,
                    "rows": 0,
                    "duplicate_keys": 0,
                    "nulls": {},
                    "error": str(exc),
                }
            )
            continue

        row_count = df.count()
        null_counts = {
            col: _null_count(df, col) for col in REQUIRED_COLUMNS.get(dataset, [])
        }

        key_cols = DATASET_KEYS.get(dataset, [])
        duplicate_count = 0
        if key_cols:
            duplicate_count = (
                df.groupBy([F.col(c) for c in key_cols])
                .count()
                .filter(F.col("count") > 1)
                .count()
            )

        records.append(
            {
                "dataset": dataset,
                "rows": row_count,
                "duplicate_keys": duplicate_count,
                "nulls": null_counts,
                "error": None,
            }
        )

    report_df = spark.createDataFrame(records)
    report_df.coalesce(1).write.mode("overwrite").json(report_path)

    print("Validation report written to:", report_path)
    for record in records:
        print(record)

    spark.stop()


if __name__ == "__main__":
    main()
