import os
import sys
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

SPARK_JOBS_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SPARK_JOBS_DIR.parent
sys.path.append(str(SPARK_JOBS_DIR))

from common.schemas import dataset_names
from common.spark_session import build_spark_session, join_path, resolve_storage_base


DATASET_KEYS = {
    "movies": ["movieId"],
    "ratings": ["userId", "movieId", "timestamp"],
    "tags": ["userId", "movieId", "tag", "timestamp"],
    "links": ["movieId"],
    "genome_scores": ["movieId", "tagId"],
    "genome_tags": ["tagId"],
    "movie_features": ["movieId"],
    "ratings_stats": ["movieId"],
    "tag_stats": ["movieId"],
}

FEATURE_DATASETS = ["movie_features", "ratings_stats", "tag_stats"]


def _resolve_paths():
    cleaned_base = resolve_storage_base(
        "MINIO_BUCKET_CLEANED",
        "MINIO_CLEANED_PREFIX",
        "CLEANED_LOCAL_DIR",
        str(REPO_ROOT / "data" / "cleaned"),
        "cleaned",
    )
    features_base = resolve_storage_base(
        "MINIO_BUCKET_FEATURES",
        "MINIO_FEATURES_PREFIX",
        "FEATURES_LOCAL_DIR",
        str(REPO_ROOT / "data" / "features"),
        "features",
    )

    report_path = os.getenv("QUALITY_REPORT_PATH", "").strip()
    if not report_path:
        report_path = join_path(features_base, "_quality/report")
    return cleaned_base, features_base, report_path


def _null_count(df, column):
    field = next((item for item in df.schema.fields if item.name == column), None)
    if field and isinstance(field.dataType, StringType):
        return df.filter(
            F.col(column).isNull() | (F.trim(F.col(column)) == "")
        ).count()
    return df.filter(F.col(column).isNull()).count()


def _duplicate_key_count(df, key_columns):
    if not key_columns:
        return 0

    return (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )


def summarize_dataset(spark, layer, dataset, base_path):
    path = join_path(base_path, dataset)
    try:
        df = spark.read.parquet(path)
        rows = df.count()
        nulls = {column: _null_count(df, column) for column in df.columns}
        duplicate_keys = _duplicate_key_count(df, DATASET_KEYS.get(dataset, []))
        return {
            "layer": layer,
            "dataset": dataset,
            "path": path,
            "rows": rows,
            "nulls": nulls,
            "duplicate_keys": duplicate_keys,
            "error": None,
        }
    except Exception as exc:
        return {
            "layer": layer,
            "dataset": dataset,
            "path": path,
            "rows": 0,
            "nulls": {},
            "duplicate_keys": 0,
            "error": str(exc),
        }


def collect_quality_records(spark, cleaned_base, features_base):
    records = []
    for dataset in dataset_names():
        records.append(summarize_dataset(spark, "cleaned", dataset, cleaned_base))
    for dataset in FEATURE_DATASETS:
        records.append(summarize_dataset(spark, "features", dataset, features_base))
    return records


def main():
    spark = build_spark_session("quality_checks")
    cleaned_base, features_base, report_path = _resolve_paths()

    records = collect_quality_records(spark, cleaned_base, features_base)
    report_df = spark.createDataFrame(records)
    report_df.coalesce(1).write.mode("overwrite").json(report_path)

    print("Quality report written to:", report_path)
    for record in records:
        print(record)

    spark.stop()


if __name__ == "__main__":
    main()
