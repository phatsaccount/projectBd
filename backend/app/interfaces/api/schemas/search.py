from pydantic import BaseModel
from typing import List, Optional


class SearchRequest(BaseModel):
    q: Optional[str] = None
    page: int = 1
    size: int = 10


class MovieHit(BaseModel):
    movieId: int
    title: Optional[str] = None
    genres: Optional[str] = None
    genres_list: Optional[List[str]] = None
    rating_count: Optional[int] = None
    rating_mean: Optional[float] = None
    tag_list: Optional[List[str]] = None


class SearchResponse(BaseModel):
    total: int
    hits: List[MovieHit]
