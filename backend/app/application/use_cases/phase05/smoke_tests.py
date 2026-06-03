"""Smoke tests for Phase 5."""

import pytest
import json
import time
from unittest.mock import Mock, MagicMock, patch
import requests
from pathlib import Path

from backend.app.infrastructure.redis.recommendation_cache import RecommendationCache
from backend.app.application.use_cases.phase05.model_loader import Phase4ArtifactLoader
from backend.app.application.use_cases.phase05.ranking import RecommendationRanker
from backend.app.application.use_cases.phase05.streaming_consumer import (
    EventStreamingConsumer,
    StreamingMetrics,
)


class TestEventIngestionAPI:
    """Smoke tests for event ingestion API."""
    
    def test_event_schema_validation(self):
        """Test that event schema validates properly."""
        from backend.app.interfaces.api.schemas.events import TrackEventRequest
        
        # Valid event
        valid_event = TrackEventRequest(
            user_id=1,
            movie_id=10,
            event_type="view",
            timestamp=1234567890,
        )
        assert valid_event.user_id == 1
        
        # Invalid event type
        with pytest.raises(ValueError):
            TrackEventRequest(
                user_id=1,
                movie_id=10,
                event_type="invalid",
            )
        
        # Rating without rate event type
        with pytest.raises(ValueError):
            TrackEventRequest(
                user_id=1,
                movie_id=10,
                event_type="view",
                rating=5,
            )


class TestRecommendationCache:
    """Smoke tests for recommendation cache."""
    
    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        client = Mock()
        client.get = Mock()
        client.set = Mock(return_value=True)
        client.delete = Mock(return_value=True)
        return client
    
    def test_cache_set_get(self, mock_redis):
        """Test cache set and get operations."""
        cache = RecommendationCache(mock_redis, ttl=3600)
        
        candidates = [
            {"movie_id": 1, "score": 0.9},
            {"movie_id": 2, "score": 0.8},
        ]
        
        # Set
        result = cache.set_recommendations(user_id=123, candidates=candidates)
        assert result is True
        
        # Get (mock returns the set data)
        payload = {
            "user_id": 123,
            "candidates": candidates,
            "version": 1,
            "computed_at": 1234567890,
        }
        mock_redis.get.return_value = json.dumps(payload)
        
        result = cache.get_recommendations(user_id=123)
        assert result is not None
        assert result["user_id"] == 123
        assert len(result["candidates"]) == 2


class TestRecommendationRanker:
    """Smoke tests for recommendation ranker."""
    
    def test_ranker_initialization(self):
        """Test ranker initialization."""
        ranker = RecommendationRanker(top_k=10)
        assert ranker.top_k == 10
        assert ranker.event_boost_factor == 1.5
    
    def test_rank_candidates(self):
        """Test ranking candidates."""
        ranker = RecommendationRanker(top_k=5)
        
        popularity_scores = {1: 0.9, 2: 0.8, 3: 0.7}
        item_neighbors = {10: [(1, 0.8), (2, 0.7)]}
        user_neighbors = {}
        
        ranked = ranker.rank_candidates(
            popularity_scores=popularity_scores,
            item_neighbors=item_neighbors,
            user_neighbors=user_neighbors,
            user_id=123,
            recent_events=[{"movie_id": 10, "event_type": "view"}],
            k=5,
        )
        
        assert len(ranked) <= 5
        assert all(isinstance(r, tuple) and len(r) == 3 for r in ranked)
        assert all(r[1] >= 0 for r in ranked)  # Scores are non-negative


class TestStreamingMetrics:
    """Smoke tests for streaming metrics."""
    
    def test_metrics_tracking(self):
        """Test metrics tracking."""
        metrics = StreamingMetrics()
        
        # Record events
        for i in range(10):
            metrics.record_event(user_id=1, event_data={"movie_id": i})
        
        assert metrics.events_processed == 10
        assert metrics.recommendations_updated == 0
        
        # Record updates
        metrics.record_recommendation_update()
        assert metrics.recommendations_updated == 1
        
        # Get stats
        stats = metrics.get_stats()
        assert stats["events_processed"] == 10
        assert stats["unique_users"] == 1


class TestIntegration:
    """Integration smoke tests."""
    
    def test_event_flow_schema_validation(self):
        """Test event schema in the full flow."""
        from backend.app.interfaces.api.schemas.events import (
            TrackEventRequest,
            EventAckResponse,
            generate_event_id,
            get_current_timestamp,
        )
        
        # Simulate event tracking
        event_id = generate_event_id()
        timestamp = get_current_timestamp()
        
        assert event_id is not None
        assert timestamp > 0
        
        # Simulate request validation
        valid_request = TrackEventRequest(
            user_id=1,
            movie_id=10,
            event_type="rate",
            rating=5,
            timestamp=timestamp,
        )
        
        # Simulate response
        response = EventAckResponse(
            event_id=event_id,
            status="accepted",
            timestamp=timestamp,
        )
        
        assert response.status == "accepted"


# Smoke tests that can run without external services
@pytest.mark.smoke
def test_artifact_loader_paths():
    """Test artifact loader path construction."""
    # This would need actual Phase 4 artifacts to fully test
    # For now, just test that the loader can be instantiated
    artifacts_dir = Path(__file__).resolve().parents[5] / "data" / "models" / "phase04"
    
    # Don't actually try to load if directory doesn't exist (for CI/CD)
    if artifacts_dir.exists():
        loader = Phase4ArtifactLoader(artifacts_dir)
        assert loader.artifacts_dir == artifacts_dir


@pytest.mark.smoke
def test_ranking_weights_normalization():
    """Test that ranker weights sum correctly."""
    ranker = RecommendationRanker()
    
    total_weight = sum(ranker.weights.values())
    assert abs(total_weight - 1.0) < 0.01  # Allow small floating point error


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "smoke"])
