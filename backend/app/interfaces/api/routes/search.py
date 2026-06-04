from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
import os

from backend.app.interfaces.api.schemas.search import SearchResponse, MovieHit
from backend.app.application.use_cases.search_movie import SearchMovieUseCase
from backend.app.infrastructure.elasticsearch.repository import ESRepository

try:
    from elasticsearch import Elasticsearch
except Exception:
    Elasticsearch = None

router = APIRouter()


def get_es_client():
    if Elasticsearch is None:
        raise RuntimeError("elasticsearch package not installed")
    host = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
    return Elasticsearch(host)


@router.get("/movies/search", response_model=SearchResponse)
def search_movies(q: Optional[str] = None, page: int = 1, size: int = 10):
    es = get_es_client()
    repo = ESRepository(es)
    uc = SearchMovieUseCase(repo)
    res = uc.search(q or "", page=page, size=size)
    hits = []
    for hit in res.get("hits", {}).get("hits", []):
        src = hit.get("_source", {})
        hits.append(MovieHit(**src))
    total = res.get("hits", {}).get("total", {}).get("value", 0)
    return {"total": total, "hits": hits}


@router.get("/movies/{movie_id}", response_model=MovieHit)
def get_movie(movie_id: int):
    es = get_es_client()
    repo = ESRepository(es)
    uc = SearchMovieUseCase(repo)
    doc = uc.get_movie(movie_id)
    if not doc:
        raise HTTPException(status_code=404, detail="movie not found")
    return MovieHit(**doc)
