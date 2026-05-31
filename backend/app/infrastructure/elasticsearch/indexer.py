"""Indexer to build the Elasticsearch movies index from cleaned Parquet or MinIO."""
import os
import sys
from pathlib import Path
from typing import Iterable

from pyspark.sql import SparkSession

SPARK_JOBS_DIR = Path(__file__).resolve().parents[4]
sys.path.append(str(SPARK_JOBS_DIR / "spark-jobs"))

from backend.app.infrastructure.elasticsearch.mappings import MOVIE_MAPPING, MOVIES_INDEX
from backend.app.infrastructure.elasticsearch.repository import ESRepository


def _build_es_client():
    try:
        from elasticsearch import Elasticsearch
    except Exception:
        raise RuntimeError("elasticsearch package not installed; install via pip")

    es_host = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
    return Elasticsearch(es_host)


def _read_movies(spark: SparkSession, cleaned_base: str) -> Iterable[dict]:
    path = os.path.join(cleaned_base, "movies")
    df = spark.read.parquet(path)
    for row in df.collect():
        yield {
            "movieId": int(row.movieId),
            "title": row.title,
            "genres": row.genres,
            "genres_list": row.get("genres_list") if "genres_list" in df.columns else [],
        }


def index_movies(cleaned_base: str, force: bool = False, sample: bool = False):
    spark = SparkSession.builder.appName("indexer").master("local[1]").getOrCreate()
    es = _build_es_client()
    repo = ESRepository(es)

    repo.create_index(MOVIE_MAPPING, force=force)

    docs = _read_movies(spark, cleaned_base)
    if sample:
        docs = list(docs)[:100]
    repo.bulk_index(docs)

    spark.stop()


if __name__ == "__main__":
    cleaned_base = os.getenv("CLEANED_LOCAL_DIR", str(Path(__file__).resolve().parents[4] / "data" / "cleaned"))
    force = os.getenv("FORCE_INDEX", "false").lower() in ("1", "true", "yes")
    sample = os.getenv("SAMPLE_INDEX", "false").lower() in ("1", "true", "yes")
    index_movies(cleaned_base, force=force, sample=sample)
