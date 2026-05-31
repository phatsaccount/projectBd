"""Elasticsearch index mapping and analyzer definitions for movie documents."""

MOVIES_INDEX = "movies_v1"

MOVIE_MAPPING = {
    "mappings": {
        "properties": {
            "movieId": {"type": "integer"},
            "title": {
                "type": "text",
                "analyzer": "standard",
                "fields": {"keyword": {"type": "keyword"}},
            },
            "genres": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "genres_list": {"type": "keyword"},
            "rating_count": {"type": "integer"},
            "rating_mean": {"type": "float"},
            "tag_list": {"type": "keyword"},
            "imdbId": {"type": "keyword"},
            "tmdbId": {"type": "keyword"},
        }
    }
}
