"""Simple Elasticsearch repository helper using elasticsearch-py.

This file avoids importing heavy clients at module import time; callers should
create the client and pass it in. Error handling is minimal but explicit.
"""
from typing import Iterable, List, Optional

import os

try:
    from elasticsearch import Elasticsearch, helpers
except Exception:
    Elasticsearch = None
    helpers = None

from .mappings import MOVIES_INDEX


class ESRepository:
    def __init__(self, client: "Elasticsearch"):
        if Elasticsearch is None:
            raise RuntimeError("elasticsearch package not installed")
        if client is None:
            raise ValueError("client is required")
        self.client = client

    def index_exists(self, index: str = MOVIES_INDEX) -> bool:
        return self.client.indices.exists(index=index)

    def create_index(self, body: dict, index: str = MOVIES_INDEX, force: bool = False):
        if self.index_exists(index):
            if force:
                self.client.indices.delete(index=index)
            else:
                return
        self.client.indices.create(index=index, body=body)

    def bulk_index(self, documents: Iterable[dict], index: str = MOVIES_INDEX, chunk_size: int = 500):
        if helpers is None:
            raise RuntimeError("elasticsearch.helpers not available")
        actions = ({"_index": index, "_id": doc.get("movieId"), "_source": doc} for doc in documents)
        helpers.bulk(self.client, actions, chunk_size=chunk_size)

    def search(self, query: dict, index: str = MOVIES_INDEX, size: int = 10, from_: int = 0) -> dict:
        return self.client.search(index=index, body=query, size=size, from_=from_)

    def get(self, movie_id: int, index: str = MOVIES_INDEX) -> Optional[dict]:
        try:
            resp = self.client.get(index=index, id=movie_id)
            return resp.get("_source")
        except Exception:
            return None
