"""Indexer to build the Elasticsearch movies index from cleaned Parquet or MinIO."""
import os
import sys
from pathlib import Path
from typing import Iterable

from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))
sys.path.append(str(REPO_ROOT / "spark-jobs"))

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
    path = Path(cleaned_base) / "movies"
    if path.exists():
        df = spark.read.parquet(str(path))
    else:
        csv_path = _resolve_movies_csv()
        df = spark.read.option("header", "true").csv(str(csv_path))

    for row in df.collect():
        record = row.asDict()
        genres = record.get("genres") or ""
        genres_list = record.get("genres_list")
        if genres_list is None:
            genres_list = [] if genres == "(no genres listed)" else genres.split("|")

        yield {
            "movieId": int(record["movieId"]),
            "title": record.get("title"),
            "genres": genres,
            "genres_list": genres_list,
        }


def _resolve_movies_csv() -> Path:
    explicit = os.getenv("MOVIES_CSV", "").strip()
    candidates = []
    if explicit:
        candidates.append(Path(explicit))
    candidates.extend(
        [
            REPO_ROOT / "data" / "raw" / "movie.csv",
            REPO_ROOT / "data" / "archive" / "movie.csv",
        ]
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Could not find movie.csv. Set MOVIES_CSV or create data/raw/movie.csv."
    )


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
    cleaned_base = os.getenv("CLEANED_LOCAL_DIR", str(REPO_ROOT / "data" / "cleaned"))
    force = os.getenv("FORCE_INDEX", "false").lower() in ("1", "true", "yes")
    sample = os.getenv("SAMPLE_INDEX", "false").lower() in ("1", "true", "yes")
    index_movies(cleaned_base, force=force, sample=sample)
