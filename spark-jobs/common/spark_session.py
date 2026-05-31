import os

from pyspark.sql import SparkSession


def _bool_env(name, default="false"):
    value = os.getenv(name, default).strip().lower()
    return value in {"1", "true", "yes"}


def build_spark_session(app_name):
    builder = SparkSession.builder.appName(app_name)

    master = os.getenv("SPARK_MASTER", "").strip()
    if master:
        builder = builder.master(master)
    else:
        builder = builder.master("local[*]")

    shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "").strip()
    if not shuffle_partitions:
        shuffle_partitions = "4"
    builder = builder.config("spark.sql.shuffle.partitions", shuffle_partitions)

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


def join_path(base, tail):
    if base.startswith("s3a://"):
        return base.rstrip("/") + "/" + tail.lstrip("/")
    return os.path.join(base, tail)


def _build_s3a_path(bucket, prefix):
    prefix = prefix.strip("/")
    if prefix:
        return f"s3a://{bucket}/{prefix}"
    return f"s3a://{bucket}"


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
        raise ValueError("Missing MinIO configuration: " + ", ".join(sorted(missing)))


def resolve_storage_base(
    bucket_env,
    prefix_env,
    local_env,
    local_default,
    default_prefix,
):
    bucket = os.getenv(bucket_env, "").strip()
    prefix = os.getenv(prefix_env, default_prefix).strip()

    if bucket:
        _validate_s3a_env()
        return _build_s3a_path(bucket, prefix)

    local_base = os.getenv(local_env, local_default)
    os.makedirs(local_base, exist_ok=True)
    return local_base
