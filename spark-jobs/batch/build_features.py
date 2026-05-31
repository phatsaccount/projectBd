import sys
from pathlib import Path

from pyspark.sql import functions as F

SPARK_JOBS_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SPARK_JOBS_DIR.parent
sys.path.append(str(SPARK_JOBS_DIR))

from common.spark_session import build_spark_session, join_path, resolve_storage_base
from common.transformations import genres_to_array


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
    return cleaned_base, features_base


def build_rating_stats(ratings_df):
    return ratings_df.groupBy("movieId").agg(
        F.count("rating").alias("rating_count"),
        F.avg("rating").alias("rating_mean"),
    )


def build_tag_stats(tags_df):
    tags_df = tags_df.filter(F.col("tag").isNotNull())
    return tags_df.groupBy("movieId").agg(
        F.count("tag").alias("tag_count"),
        F.sort_array(F.array_distinct(F.collect_list("tag"))).alias("tag_list"),
    )


def build_movie_features(movies_df, rating_stats_df, tag_stats_df):
    movies_df = genres_to_array(movies_df, "genres", "genres_list")
    features = (
        movies_df.join(rating_stats_df, "movieId", "left")
        .join(tag_stats_df, "movieId", "left")
        .withColumn("rating_count", F.coalesce(F.col("rating_count"), F.lit(0)))
        .withColumn("tag_count", F.coalesce(F.col("tag_count"), F.lit(0)))
        .withColumn(
            "tag_list",
            F.when(F.col("tag_list").isNull(), F.expr("array()")).otherwise(
                F.col("tag_list")
            ),
        )
    )

    return features.select(
        "movieId",
        "title",
        "genres",
        "genres_list",
        "rating_count",
        "rating_mean",
        "tag_count",
        "tag_list",
    )


def main():
    spark = build_spark_session("build_features")
    cleaned_base, features_base = _resolve_paths()

    movies = spark.read.parquet(join_path(cleaned_base, "movies"))
    ratings = spark.read.parquet(join_path(cleaned_base, "ratings"))
    tags = spark.read.parquet(join_path(cleaned_base, "tags"))

    rating_stats = build_rating_stats(ratings)
    tag_stats = build_tag_stats(tags)
    movie_features = build_movie_features(movies, rating_stats, tag_stats)

    movie_features.write.mode("overwrite").parquet(
        join_path(features_base, "movie_features")
    )
    rating_stats.write.mode("overwrite").parquet(
        join_path(features_base, "ratings_stats")
    )
    tag_stats.write.mode("overwrite").parquet(join_path(features_base, "tag_stats"))

    print(f"Features written to {features_base}")
    spark.stop()


if __name__ == "__main__":
    main()
