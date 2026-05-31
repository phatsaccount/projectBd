from pydantic import BaseModel
from typing import List, Optional


class SearchRequest(BaseModel):
    q: Optional[str] = None
    page: int = 1
    size: int = 10


class MovieHit(BaseModel):
    movieId: int
    title: Optional[str]
    genres: Optional[str]
    genres_list: Optional[List[str]]
    rating_count: Optional[int]
    rating_mean: Optional[float]
    tag_list: Optional[List[str]]


class SearchResponse(BaseModel):
    total: int
    hits: List[MovieHit]
