"""Recommendation serving API routes."""

from fastapi import APIRouter, HTTPException, Depends
from typing import Optional
import logging
import json
import os
from datetime import datetime
from pathlib import Path

from backend.app.interfaces.api.schemas.recommendations import (
    RecommendationResponse,
    RecommendationItem,
)
from backend.app.infrastructure.redis.recommendation_cache import RecommendationCache
from backend.app.application.use_cases.phase05.model_loader import Phase4ArtifactLoader
from backend.app.application.use_cases.phase05.ranking import RecommendationRanker

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/recommendations", tags=["recommendations"])

# Global instances
_cache: Optional[RecommendationCache] = None
_artifact_loader: Optional[Phase4ArtifactLoader] = None
_ranker: Optional[RecommendationRanker] = None


def get_cache() -> RecommendationCache:
    """Dependency: get the recommendation cache."""
    if _cache is None:
        raise RuntimeError("Recommendation cache not initialized")
    return _cache


def initialize_recommendation_api(
    cache: RecommendationCache,
    artifacts_dir: Optional[Path] = None,
) -> None:
    """Initialize the recommendation API.
    
    Args:
        cache: RecommendationCache instance
        artifacts_dir: Path to Phase 4 artifacts
    """
    global _cache, _artifact_loader, _ranker
    
    _cache = cache
    
    # Initialize artifact loader
    if artifacts_dir is None:
        artifacts_dir_env = os.getenv("PHASE04_ARTIFACTS_DIR", "").strip()
        if artifacts_dir_env:
            artifacts_dir = Path(artifacts_dir_env)
        else:
            artifacts_dir = Path(__file__).resolve().parents[5] / "data" / "models" / "phase04"
    
    if artifacts_dir.exists():
        _artifact_loader = Phase4ArtifactLoader(artifacts_dir)
    else:
        _artifact_loader = None
        logger.warning("Phase 4 artifacts directory not found: %s", artifacts_dir)

    _ranker = RecommendationRanker(top_k=10)
    
    logger.info(f"Recommendation API initialized: artifacts_dir={artifacts_dir}")


@router.get("/{user_id}", response_model=RecommendationResponse)
def get_recommendations(
    user_id: int,
    k: int = 10,
    cache: RecommendationCache = Depends(get_cache),
) -> RecommendationResponse:
    """Get recommendations for a user.
    
    Attempts to retrieve recommendations from cache first. If cache miss,
    computes recommendations from Phase 4 static artifacts.
    
    Args:
        user_id: User ID to get recommendations for
        k: Number of recommendations to return (default: 10)
        cache: Recommendation cache instance
        
    Returns:
        RecommendationResponse with top-K recommendations
        
    Raises:
        HTTPException: If user_id is invalid or error occurs
    """
    try:
        if user_id <= 0:
            raise HTTPException(status_code=400, detail="user_id must be positive")
        
        timestamp = int(datetime.utcnow().timestamp())
        cached = False
        recommendations = []
        
        # Try to get from cache
        cached_data = cache.get_recommendations(user_id)
        
        if cached_data:
            # Cache hit: use cached recommendations
            cached = True
            candidates = cached_data.get("candidates", [])
            logger.debug(f"Cache hit for user {user_id}: {len(candidates)} candidates")
        else:
            # Cache miss: compute from Phase 4 artifacts (fallback)
            candidates = _compute_fallback_recommendations(user_id, k)
            logger.debug(f"Cache miss for user {user_id}: computed {len(candidates)} fallback candidates")
        
        # Build response items
        for rank, candidate in enumerate(candidates[:k], 1):
            item = RecommendationItem(
                movie_id=candidate["movie_id"],
                score=float(candidate["score"]),
                rank=rank,
                reason=candidate.get("reason", "collaborative"),
            )
            recommendations.append(item)
        
        return RecommendationResponse(
            user_id=user_id,
            recommendations=recommendations,
            total=len(recommendations),
            cached=cached,
            timestamp=timestamp,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting recommendations for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


def _compute_fallback_recommendations(user_id: int, k: int) -> list:
    """Compute recommendations from Phase 4 artifacts (fallback).
    
    Args:
        user_id: User ID
        k: Number of recommendations
        
    Returns:
        List of candidate recommendations
    """
    try:
        if _artifact_loader is None:
            logger.error("Artifact loader not initialized")
            return []
        
        # Load popularity artifacts
        popularity_df = _artifact_loader.load_popularity()
        if popularity_df is None or popularity_df.empty:
            logger.warning("No popularity data available")
            return []
        
        # Convert to candidates format
        candidates = []
        max_score = float(popularity_df["popularity_score"].max())
        for _, row in popularity_df.iterrows():
            raw_score = float(row["popularity_score"])
            candidates.append({
                "movie_id": int(row["movie_id"]),
                "score": raw_score / max_score if max_score > 0 else 0.0,
                "reason": "popularity",
            })
        
        # Sort by score descending
        candidates.sort(key=lambda x: x["score"], reverse=True)
        
        # Return top-k
        return candidates[:k]
        
    except Exception as e:
        logger.error(f"Error computing fallback recommendations: {e}")
        return []
