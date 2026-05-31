from typing import List, Optional

from backend.app.infrastructure.elasticsearch.repository import ESRepository


class SearchMovieUseCase:
    def __init__(self, repo: ESRepository):
        self.repo = repo

    def search(self, q: str, page: int = 1, size: int = 10) -> dict:
        if not q:
            # return match_all
            query = {"query": {"match_all": {}}}
        else:
            query = {"query": {"multi_match": {"query": q, "fields": ["title^2", "genres", "tag_list"]}}}
        from_ = max(0, (page - 1) * size)
        return self.repo.search(query, size=size, from_=from_)

    def get_movie(self, movie_id: int) -> Optional[dict]:
        return self.repo.get(movie_id)
