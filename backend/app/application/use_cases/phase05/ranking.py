"""Event-based re-ranking logic for recommendations."""

import logging
from typing import Dict, List, Tuple, Optional
import numpy as np

logger = logging.getLogger(__name__)


class RecommendationRanker:
    """Re-rank recommendation candidates based on user events."""
    
    def __init__(
        self,
        weights: Optional[Dict[str, float]] = None,
        event_boost_factor: float = 1.5,
        top_k: int = 10,
    ):
        """Initialize ranker.
        
        Args:
            weights: Weights for each signal (popularity, item, user, content)
            event_boost_factor: Boost factor for items similar to recent events
            top_k: Number of top recommendations to return
        """
        if weights is None:
            weights = {
                "popularity": 0.2,
                "item": 0.3,
                "user": 0.2,
                "content": 0.3,
            }
        
        self.weights = weights
        self.event_boost_factor = event_boost_factor
        self.top_k = top_k
        
        logger.info(f"RecommendationRanker initialized: weights={weights}, boost={event_boost_factor}, k={top_k}")
    
    def rank_candidates(
        self,
        popularity_scores: Dict[int, float],
        item_neighbors: Dict[int, List[Tuple[int, float]]],
        user_neighbors: Dict[int, List[Tuple[int, float]]],
        user_id: int,
        recent_events: List[Dict],
        k: Optional[int] = None,
    ) -> List[Tuple[int, float, str]]:
        """Rank candidates combining multiple signals and event-based boosting.
        
        Args:
            popularity_scores: Dict of movie_id -> popularity score
            item_neighbors: Dict of movie_id -> [(neighbor_id, score), ...]
            user_neighbors: Dict of movie_id -> [(neighbor_id, score), ...]
            user_id: Target user ID
            recent_events: List of recent user events
            k: Number of top recommendations (uses self.top_k if None)
            
        Returns:
            List of (movie_id, final_score, reason) tuples, sorted by score desc
        """
        k = k or self.top_k
        
        # Collect all candidate movies
        all_candidates = set()
        
        # Add movies from popularity
        all_candidates.update(popularity_scores.keys())
        
        # Add movies from item neighbors
        for neighbors in item_neighbors.values():
            for neighbor_id, _ in neighbors:
                all_candidates.add(neighbor_id)
        
        # Add movies from user neighbors
        for neighbors in user_neighbors.values():
            for neighbor_id, _ in neighbors:
                all_candidates.add(neighbor_id)
        
        # Filter out movies user has interacted with recently
        recent_movie_ids = {event["movie_id"] for event in recent_events}
        all_candidates = all_candidates - recent_movie_ids
        
        if not all_candidates:
            logger.debug(f"No candidates for user {user_id}")
            return []
        
        # Score each candidate
        scored_candidates = []
        for movie_id in all_candidates:
            score = self._compute_candidate_score(
                movie_id,
                popularity_scores,
                item_neighbors,
                user_neighbors,
                user_id,
                recent_events,
            )
            
            # Determine primary reason
            reason = self._determine_reason(movie_id, item_neighbors, user_neighbors)
            
            scored_candidates.append((movie_id, score, reason))
        
        # Sort by score descending
        scored_candidates.sort(key=lambda x: x[1], reverse=True)
        
        # Return top-k
        result = scored_candidates[:k]
        logger.debug(f"Ranked {len(result)} candidates for user {user_id}")
        return result
    
    def _compute_candidate_score(
        self,
        movie_id: int,
        popularity_scores: Dict[int, float],
        item_neighbors: Dict[int, List[Tuple[int, float]]],
        user_neighbors: Dict[int, List[Tuple[int, float]]],
        user_id: int,
        recent_events: List[Dict],
    ) -> float:
        """Compute weighted score for a candidate movie.
        
        Args:
            movie_id: Movie ID to score
            popularity_scores: Popularity scores
            item_neighbors: Item neighbors mapping
            user_neighbors: User neighbors mapping
            user_id: Target user ID
            recent_events: Recent user events
            
        Returns:
            Weighted final score
        """
        score = 0.0
        
        # Popularity signal
        pop_score = popularity_scores.get(movie_id, 0.0)
        score += self.weights["popularity"] * pop_score
        
        # Item-based signal: how many times is this movie a neighbor of recent items?
        item_boost = self._compute_item_boost(movie_id, item_neighbors, recent_events)
        score += self.weights["item"] * item_boost
        
        # User-based signal: how many user neighbors liked this?
        user_boost = self._compute_user_boost(movie_id, user_neighbors, user_id)
        score += self.weights["user"] * user_boost
        
        # Content-based signal (placeholder for now)
        content_boost = 0.0  # Would use embeddings in full implementation
        score += self.weights["content"] * content_boost
        
        return score
    
    def _compute_item_boost(
        self,
        movie_id: int,
        item_neighbors: Dict[int, List[Tuple[int, float]]],
        recent_events: List[Dict],
    ) -> float:
        """Compute item-based boost score.
        
        Args:
            movie_id: Movie to score
            item_neighbors: Item neighbors mapping
            recent_events: Recent user events
            
        Returns:
            Boost score (0-1)
        """
        recent_movie_ids = [event["movie_id"] for event in recent_events[-10:]]  # Last 10 events
        
        boost = 0.0
        for recent_id in recent_movie_ids:
            neighbors = item_neighbors.get(recent_id, [])
            for neighbor_id, score in neighbors:
                if neighbor_id == movie_id:
                    boost += score * self.event_boost_factor
        
        return min(boost, 1.0)  # Cap at 1.0
    
    def _compute_user_boost(
        self,
        movie_id: int,
        user_neighbors: Dict[int, List[Tuple[int, float]]],
        user_id: int,
    ) -> float:
        """Compute user-based boost score.
        
        Args:
            movie_id: Movie to score
            user_neighbors: User neighbors mapping
            user_id: Target user ID
            
        Returns:
            Boost score (0-1)
        """
        neighbors = user_neighbors.get(user_id, [])
        
        boost = 0.0
        for neighbor_id, score in neighbors:
            neighbor_movies = user_neighbors.get(neighbor_id, [])
            for movie, m_score in neighbor_movies:
                if movie == movie_id:
                    boost += m_score
        
        return min(boost, 1.0)  # Cap at 1.0
    
    def _determine_reason(
        self,
        movie_id: int,
        item_neighbors: Dict[int, List[Tuple[int, float]]],
        user_neighbors: Dict[int, List[Tuple[int, float]]],
    ) -> str:
        """Determine primary reason for recommendation.
        
        Args:
            movie_id: Movie ID
            item_neighbors: Item neighbors mapping
            user_neighbors: User neighbors mapping
            
        Returns:
            Reason string: 'collaborative', 'content_based', or 'popularity'
        """
        # Simplified logic; in real system would track which signal dominated
        if user_neighbors:
            return "collaborative"
        elif item_neighbors:
            return "content_based"
        else:
            return "popularity"
