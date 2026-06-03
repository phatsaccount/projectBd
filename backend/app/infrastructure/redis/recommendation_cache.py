"""Recommendation cache layer using Redis."""

import json
import logging
from typing import Optional, List, Dict, Any

from backend.app.infrastructure.redis.client import RedisClient

logger = logging.getLogger(__name__)


class RecommendationCache:
    """Cache for storing and retrieving user recommendations."""
    
    # Redis key pattern
    KEY_PREFIX = "recommendations"
    DEFAULT_TTL = 3600  # 1 hour
    
    def __init__(self, redis_client: RedisClient, ttl: int = DEFAULT_TTL):
        """Initialize recommendation cache.
        
        Args:
            redis_client: RedisClient instance
            ttl: TTL for recommendations in seconds (default: 1 hour)
        """
        self.redis = redis_client
        self.ttl = ttl
    
    def _get_key(self, user_id: int) -> str:
        """Generate Redis key for user recommendations.
        
        Args:
            user_id: User ID
            
        Returns:
            Redis key
        """
        return f"{self.KEY_PREFIX}:{user_id}"
    
    def set_recommendations(
        self,
        user_id: int,
        candidates: List[Dict[str, Any]],
        version: int = 1,
        ttl: Optional[int] = None,
    ) -> bool:
        """Store recommendations for a user.
        
        Args:
            user_id: User ID
            candidates: List of candidate movies with scores
                Format: [{"movie_id": int, "score": float}, ...]
            version: Version number for invalidation
            ttl: TTL in seconds (uses default if None)
            
        Returns:
            True if successful
        """
        try:
            from datetime import datetime
            
            key = self._get_key(user_id)
            ttl_to_use = ttl or self.ttl
            
            payload = {
                "user_id": user_id,
                "candidates": candidates,
                "version": version,
                "computed_at": int(datetime.utcnow().timestamp()),
            }
            
            value = json.dumps(payload)
            success = self.redis.set(key, value, ex=ttl_to_use)
            
            if success:
                logger.debug(f"Set recommendations for user {user_id}: {len(candidates)} candidates, TTL={ttl_to_use}s")
            else:
                logger.error(f"Failed to set recommendations for user {user_id}")
            
            return success
        except Exception as e:
            logger.error(f"Error setting recommendations for user {user_id}: {e}")
            return False
    
    def get_recommendations(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve recommendations for a user.
        
        Args:
            user_id: User ID
            
        Returns:
            Dict with candidates and metadata, or None if not found
        """
        try:
            key = self._get_key(user_id)
            value = self.redis.get(key)
            
            if value is None:
                logger.debug(f"Cache miss for user {user_id}")
                return None
            
            payload = json.loads(value)
            logger.debug(f"Cache hit for user {user_id}: {len(payload.get('candidates', []))} candidates")
            return payload
        except Exception as e:
            logger.error(f"Error getting recommendations for user {user_id}: {e}")
            return None
    
    def clear_recommendations(self, user_id: int) -> bool:
        """Clear recommendations for a user.
        
        Args:
            user_id: User ID
            
        Returns:
            True if successful
        """
        try:
            key = self._get_key(user_id)
            success = self.redis.delete(key)
            if success:
                logger.debug(f"Cleared recommendations for user {user_id}")
            return success
        except Exception as e:
            logger.error(f"Error clearing recommendations for user {user_id}: {e}")
            return False
    
    def flush_all_recommendations(self) -> bool:
        """Clear all recommendations (for testing/rollback).
        
        WARNING: This will delete ALL keys in Redis database!
        
        Returns:
            True if successful
        """
        try:
            logger.warning("Flushing all recommendations from Redis")
            return self.redis.flushdb()
        except Exception as e:
            logger.error(f"Error flushing recommendations: {e}")
            return False
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics.
        
        Returns:
            Dict with cache stats
        """
        try:
            client = self.redis.client
            info = client.info()
            return {
                "memory_used": info.get("used_memory_human", "N/A"),
                "connected_clients": info.get("connected_clients", 0),
                "total_commands_processed": info.get("total_commands_processed", 0),
            }
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {}


# Global cache instance
_cache: Optional[RecommendationCache] = None


def get_recommendation_cache() -> RecommendationCache:
    """Get the global recommendation cache instance."""
    global _cache
    if _cache is None:
        raise RuntimeError("Recommendation cache not initialized")
    return _cache


def initialize_recommendation_cache(
    redis_client: RedisClient,
    ttl: int = RecommendationCache.DEFAULT_TTL,
) -> RecommendationCache:
    """Initialize the global recommendation cache."""
    global _cache
    _cache = RecommendationCache(redis_client, ttl=ttl)
    return _cache
