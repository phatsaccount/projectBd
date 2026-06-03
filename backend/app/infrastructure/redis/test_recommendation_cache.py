"""Unit tests for Redis recommendation cache."""

import json
import time
import pytest
from unittest.mock import Mock, MagicMock, patch

from backend.app.infrastructure.redis.recommendation_cache import (
    RecommendationCache,
)


class TestRecommendationCache:
    """Test recommendation cache operations."""
    
    @pytest.fixture
    def mock_redis_client(self):
        """Create a mock Redis client."""
        client = Mock()
        client.get = Mock()
        client.set = Mock(return_value=True)
        client.delete = Mock(return_value=True)
        client.exists = Mock(return_value=True)
        client.flushdb = Mock(return_value=True)
        client.ttl = Mock(return_value=3600)
        return client
    
    @pytest.fixture
    def cache(self, mock_redis_client):
        """Create a recommendation cache with mock client."""
        return RecommendationCache(mock_redis_client, ttl=3600)
    
    def test_set_recommendations_success(self, cache, mock_redis_client):
        """Test setting recommendations successfully."""
        user_id = 123
        candidates = [
            {"movie_id": 1, "score": 0.9},
            {"movie_id": 2, "score": 0.8},
        ]
        
        result = cache.set_recommendations(user_id, candidates)
        
        assert result is True
        mock_redis_client.set.assert_called_once()
        call_args = mock_redis_client.set.call_args
        assert call_args[0][0] == f"recommendations:{user_id}"
        assert call_args[1]["ex"] == 3600
    
    def test_set_recommendations_with_custom_ttl(self, cache, mock_redis_client):
        """Test setting recommendations with custom TTL."""
        user_id = 123
        candidates = [{"movie_id": 1, "score": 0.9}]
        custom_ttl = 7200
        
        result = cache.set_recommendations(user_id, candidates, ttl=custom_ttl)
        
        assert result is True
        call_args = mock_redis_client.set.call_args
        assert call_args[1]["ex"] == custom_ttl
    
    def test_set_recommendations_with_version(self, cache, mock_redis_client):
        """Test setting recommendations with version number."""
        user_id = 123
        candidates = [{"movie_id": 1, "score": 0.9}]
        version = 2
        
        result = cache.set_recommendations(user_id, candidates, version=version)
        
        assert result is True
        call_args = mock_redis_client.set.call_args
        payload = json.loads(call_args[0][1])
        assert payload["version"] == version
    
    def test_get_recommendations_cache_hit(self, cache, mock_redis_client):
        """Test getting recommendations when cache hit."""
        user_id = 123
        candidates = [
            {"movie_id": 1, "score": 0.9},
            {"movie_id": 2, "score": 0.8},
        ]
        payload = {
            "user_id": user_id,
            "candidates": candidates,
            "version": 1,
            "computed_at": 1234567890,
        }
        mock_redis_client.get.return_value = json.dumps(payload)
        
        result = cache.get_recommendations(user_id)
        
        assert result is not None
        assert result["user_id"] == user_id
        assert len(result["candidates"]) == 2
        assert result["version"] == 1
        mock_redis_client.get.assert_called_once_with(f"recommendations:{user_id}")
    
    def test_get_recommendations_cache_miss(self, cache, mock_redis_client):
        """Test getting recommendations when cache miss."""
        mock_redis_client.get.return_value = None
        
        result = cache.get_recommendations(456)
        
        assert result is None
        mock_redis_client.get.assert_called_once()
    
    def test_clear_recommendations(self, cache, mock_redis_client):
        """Test clearing recommendations for a user."""
        user_id = 123
        
        result = cache.clear_recommendations(user_id)
        
        assert result is True
        mock_redis_client.delete.assert_called_once_with(f"recommendations:{user_id}")
    
    def test_flush_all_recommendations(self, cache, mock_redis_client):
        """Test flushing all recommendations."""
        result = cache.flush_all_recommendations()
        
        assert result is True
        mock_redis_client.flushdb.assert_called_once()
    
    def test_set_recommendations_error_handling(self, cache, mock_redis_client):
        """Test error handling when setting recommendations fails."""
        mock_redis_client.set.return_value = False
        
        result = cache.set_recommendations(123, [{"movie_id": 1, "score": 0.9}])
        
        assert result is False
    
    def test_get_recommendations_error_handling(self, cache, mock_redis_client):
        """Test error handling when getting recommendations fails."""
        mock_redis_client.get.side_effect = Exception("Redis connection error")
        
        result = cache.get_recommendations(123)
        
        assert result is None
    
    def test_get_key_format(self, cache):
        """Test the key format generation."""
        user_id = 12345
        key = cache._get_key(user_id)
        
        assert key == "recommendations:12345"
    
    def test_payload_structure(self, cache, mock_redis_client):
        """Test the payload structure stored in Redis."""
        user_id = 123
        candidates = [{"movie_id": 1, "score": 0.95}]
        
        cache.set_recommendations(user_id, candidates, version=3)
        
        call_args = mock_redis_client.set.call_args
        payload = json.loads(call_args[0][1])
        
        assert "user_id" in payload
        assert "candidates" in payload
        assert "version" in payload
        assert "computed_at" in payload
        assert payload["user_id"] == user_id
        assert payload["candidates"] == candidates
        assert payload["version"] == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
