import sys
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
SPARK_JOBS_DIR = REPO_ROOT / "spark-jobs"
sys.path.append(str(SPARK_JOBS_DIR))

from common.schemas import dataset_names, get_schema
from common.spark_session import build_spark_session, join_path, resolve_storage_base
from common.transformations import (
    empty_to_null,
    normalize_genres,
    normalize_tag_column,
    trim_string_columns,
)


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


def _resolve_paths():
    landing_base = resolve_storage_base(
        "MINIO_BUCKET_LANDING",
        "MINIO_LANDING_PREFIX",
        "LANDING_LOCAL_DIR",
        str(REPO_ROOT / "data" / "landing"),
        "landing",
    )
    cleaned_base = resolve_storage_base(
        "MINIO_BUCKET_CLEANED",
        "MINIO_CLEANED_PREFIX",
        "CLEANED_LOCAL_DIR",
        str(REPO_ROOT / "data" / "cleaned"),
        "cleaned",
    )
    return landing_base, cleaned_base


def _string_columns(df):
    return [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]


def _dedupe_by_keys(df, key_columns, order_columns=None):
    if not key_columns:
        return df

    if order_columns is None:
        order_columns = [F.col(column).asc_nulls_last() for column in df.columns]

    window = Window.partitionBy(*key_columns).orderBy(*order_columns)
    return (
        df.withColumn("_row", F.row_number().over(window))
        .filter(F.col("_row") == 1)
        .drop("_row")
    )


def _filter_required(df, dataset):
    for column in REQUIRED_COLUMNS.get(dataset, []):
        df = df.filter(F.col(column).isNotNull())
    return df


def _clean_movies(df):
    df = trim_string_columns(df, ["title", "genres"])
    df = empty_to_null(df, ["title", "genres"])
    df = normalize_genres(df, "genres", "genres")
    df = df.filter(F.col("movieId").isNotNull())
    df = df.filter(F.col("title").isNotNull())

    order_columns = [
        F.col("title").asc_nulls_last(),
        F.col("genres").asc_nulls_last(),
    ]
    df = _dedupe_by_keys(df, ["movieId"], order_columns)
    return df.select("movieId", "title", "genres")


def _clean_tags(df):
    df = trim_string_columns(df, ["tag"])
    df = empty_to_null(df, ["tag"])
    df = normalize_tag_column(df, "tag")
    df = _filter_required(df, "tags")

    window = Window.partitionBy("userId", "movieId", "tag").orderBy(
        F.col("timestamp").asc_nulls_last()
    )
    return (
        df.withColumn("_row", F.row_number().over(window))
        .filter(F.col("_row") == 1)
        .drop("_row")
    )


def _clean_links(df):
    df = trim_string_columns(df, ["imdbId", "tmdbId"])
    df = empty_to_null(df, ["imdbId", "tmdbId"])
    df = df.filter(F.col("movieId").isNotNull())

    order_columns = [
        F.col("tmdbId").isNull().asc(),
        F.col("imdbId").isNull().asc(),
        F.col("imdbId").asc_nulls_last(),
        F.col("tmdbId").asc_nulls_last(),
    ]
    return _dedupe_by_keys(df, ["movieId"], order_columns)


def _clean_genome_tags(df):
    df = trim_string_columns(df, ["tag"])
    df = empty_to_null(df, ["tag"])
    df = normalize_tag_column(df, "tag")
    df = _filter_required(df, "genome_tags")

    order_columns = [F.col("tag").asc_nulls_last()]
    return _dedupe_by_keys(df, ["tagId"], order_columns)


def _clean_default(df, dataset):
    string_columns = _string_columns(df)
    df = trim_string_columns(df, string_columns)
    df = empty_to_null(df, string_columns)
    df = _filter_required(df, dataset)

    key_columns = DATASET_KEYS.get(dataset, [])
    return _dedupe_by_keys(df, key_columns)


def main():
    spark = build_spark_session("clean_movie_data")
    landing_base, cleaned_base = _resolve_paths()

    for dataset in dataset_names():
        source_path = join_path(landing_base, dataset)
        df = spark.read.schema(get_schema(dataset)).parquet(source_path)

        if dataset == "movies":
            cleaned = _clean_movies(df)
        elif dataset == "tags":
            cleaned = _clean_tags(df)
        elif dataset == "links":
            cleaned = _clean_links(df)
        elif dataset == "genome_tags":
            cleaned = _clean_genome_tags(df)
        else:
            cleaned = _clean_default(df, dataset)

        target_path = join_path(cleaned_base, dataset)
        cleaned.write.mode("overwrite").parquet(target_path)
        print(f"Cleaned {dataset} -> {target_path}")

    spark.stop()


if __name__ == "__main__":
    main()
