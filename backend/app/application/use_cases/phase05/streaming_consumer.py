"""Kafka streaming consumer for event processing and recommendation updates."""

import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
except ImportError:
    KafkaConsumer = None
    KafkaError = None

from backend.app.application.use_cases.phase05.model_loader import Phase4ArtifactLoader
from backend.app.application.use_cases.phase05.ranking import RecommendationRanker
from backend.app.infrastructure.redis.client import RedisClient
from backend.app.infrastructure.redis.recommendation_cache import RecommendationCache

logger = logging.getLogger(__name__)


class StreamingMetrics:
    """Track streaming job metrics."""
    
    def __init__(self):
        """Initialize metrics."""
        self.events_processed = 0
        self.recommendations_updated = 0
        self.errors = 0
        self.start_time = time.time()
        self.user_events: Dict[int, List[Dict]] = defaultdict(list)
    
    def record_event(self, user_id: int, event_data: Dict) -> None:
        """Record an event."""
        self.events_processed += 1
        self.user_events[user_id].append(event_data)
    
    def record_recommendation_update(self) -> None:
        """Record a recommendation update."""
        self.recommendations_updated += 1
    
    def record_error(self) -> None:
        """Record an error."""
        self.errors += 1
    
    def get_stats(self) -> Dict:
        """Get current metrics."""
        elapsed = time.time() - self.start_time
        throughput = self.events_processed / elapsed if elapsed > 0 else 0
        
        return {
            "events_processed": self.events_processed,
            "recommendations_updated": self.recommendations_updated,
            "errors": self.errors,
            "elapsed_seconds": elapsed,
            "throughput_events_per_sec": throughput,
            "unique_users": len(self.user_events),
        }


class EventStreamingConsumer:
    """Consume events from Kafka and update recommendations."""
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "user-events",
        group_id: str = "recommendation-processor",
        artifacts_dir: Optional[Path] = None,
    ):
        """Initialize streaming consumer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Kafka topic to consume
            group_id: Consumer group ID
            artifacts_dir: Path to Phase 4 artifacts
        """
        if KafkaConsumer is None:
            raise RuntimeError("kafka-python package not installed")
        
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        
        # Initialize artifact loader
        if artifacts_dir is None:
            artifacts_dir = Path(__file__).resolve().parents[5] / "data" / "models" / "phase04"
        
        self.artifact_loader = Phase4ArtifactLoader(artifacts_dir)
        
        # Initialize ranker with Phase 4 artifacts
        self.ranker = RecommendationRanker(top_k=10)
        
        # Initialize Redis client and cache
        self.redis_client = None
        self.cache = None
        
        # Metrics
        self.metrics = StreamingMetrics()
        
        # Consumer
        self.consumer = None
        
        logger.info(
            f"EventStreamingConsumer initialized: "
            f"topic={topic}, bootstrap_servers={bootstrap_servers}, "
            f"artifacts_dir={artifacts_dir}"
        )
    
    def initialize_redis(self, redis_host: str = "localhost", redis_port: int = 6379) -> None:
        """Initialize Redis connection and cache.
        
        Args:
            redis_host: Redis host
            redis_port: Redis port
        """
        try:
            self.redis_client = RedisClient(host=redis_host, port=redis_port, db=0)
            self.cache = RecommendationCache(self.redis_client, ttl=3600)
            logger.info(f"Redis initialized: {redis_host}:{redis_port}")
        except Exception as e:
            logger.error(f"Failed to initialize Redis: {e}")
            raise
    
    def verify_artifacts(self) -> bool:
        """Verify Phase 4 artifacts are available.
        
        Returns:
            True if all artifacts present
        """
        return self.artifact_loader.verify_artifacts()
    
    def start_consuming(self, max_events: Optional[int] = None) -> None:
        """Start consuming events from Kafka.
        
        Args:
            max_events: Maximum number of events to process (None = infinite)
        """
        if self.cache is None:
            raise RuntimeError("Redis not initialized. Call initialize_redis() first.")
        
        try:
            # Load Phase 4 artifacts
            logger.info("Loading Phase 4 artifacts...")
            artifacts = self.artifact_loader.load_all_artifacts()
            
            # Initialize Kafka consumer
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers.split(","),
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                max_poll_records=100,
                session_timeout_ms=30000,
            )
            
            logger.info(f"Kafka consumer started: topic={self.topic}, group={self.group_id}")
            
            # Consume events
            event_count = 0
            event_window: Dict[int, List[Dict]] = defaultdict(list)
            window_start_time = time.time()
            window_duration = 300  # 5 minutes
            
            for message in self.consumer:
                try:
                    event_data = message.value
                    user_id = event_data.get("user_id")
                    
                    # Record event
                    self.metrics.record_event(user_id, event_data)
                    event_window[user_id].append(event_data)
                    event_count += 1
                    
                    # Log progress
                    if event_count % 100 == 0:
                        stats = self.metrics.get_stats()
                        logger.info(f"Processed {event_count} events: {stats}")
                    
                    # Check if window should flush
                    elapsed = time.time() - window_start_time
                    if elapsed >= window_duration or event_count >= 1000:
                        self._process_event_window(event_window, artifacts)
                        event_window.clear()
                        window_start_time = time.time()
                    
                    # Check max events limit
                    if max_events and event_count >= max_events:
                        logger.info(f"Reached max events ({max_events}), stopping")
                        break
                    
                except Exception as e:
                    logger.error(f"Error processing event: {e}")
                    self.metrics.record_error()
                    continue
            
            # Process remaining events
            if event_window:
                self._process_event_window(event_window, artifacts)
            
            # Log final stats
            stats = self.metrics.get_stats()
            logger.info(f"Streaming consumer stopped. Final stats: {stats}")
            
        except Exception as e:
            logger.error(f"Streaming consumer error: {e}")
            raise
        finally:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")
    
    def _process_event_window(
        self,
        event_window: Dict[int, List[Dict]],
        artifacts: Dict,
    ) -> None:
        """Process a window of events and update recommendations.
        
        Args:
            event_window: Dict of user_id -> [events]
            artifacts: Phase 4 artifacts
        """
        for user_id, events in event_window.items():
            try:
                # Get artifact data
                popularity_scores = self._prepare_popularity_scores(artifacts.get("popularity"))
                item_neighbors = artifacts.get("item_neighbors", {})
                user_neighbors = artifacts.get("user_neighbors", {})
                
                # Rank candidates based on events
                ranked = self.ranker.rank_candidates(
                    popularity_scores=popularity_scores,
                    item_neighbors=item_neighbors,
                    user_neighbors=user_neighbors,
                    user_id=user_id,
                    recent_events=events,
                    k=10,
                )
                
                # Store in Redis cache
                if ranked:
                    candidates = [
                        {"movie_id": mid, "score": float(score)}
                        for mid, score, _ in ranked
                    ]
                    
                    success = self.cache.set_recommendations(
                        user_id=user_id,
                        candidates=candidates,
                        version=1,
                        ttl=3600,
                    )
                    
                    if success:
                        self.metrics.record_recommendation_update()
                        logger.debug(f"Updated recommendations for user {user_id}: {len(candidates)} candidates")
                
            except Exception as e:
                logger.error(f"Error processing events for user {user_id}: {e}")
                self.metrics.record_error()
    
    def _prepare_popularity_scores(self, popularity_df) -> Dict[int, float]:
        """Prepare popularity scores from DataFrame.
        
        Args:
            popularity_df: Popularity DataFrame
            
        Returns:
            Dict of movie_id -> score
        """
        if popularity_df is None:
            return {}
        
        try:
            # Normalize popularity scores to 0-1 range
            movie_id_column = "movie_id" if "movie_id" in popularity_df.columns else "movieId"
            score_column = (
                "popularity_score"
                if "popularity_score" in popularity_df.columns
                else "score_popularity"
            )
            if movie_id_column not in popularity_df.columns or score_column not in popularity_df.columns:
                logger.warning("Popularity artifact columns not recognized: %s", list(popularity_df.columns))
                return {}

            max_pop = popularity_df[score_column].max()
            if max_pop <= 0:
                return {}
            
            scores = {}
            for _, row in popularity_df.iterrows():
                movie_id = int(row[movie_id_column])
                score = float(row[score_column]) / max_pop
                scores[movie_id] = score
            
            return scores
        except Exception as e:
            logger.error(f"Error preparing popularity scores: {e}")
            return {}


def main():
    """Run the streaming consumer."""
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description="Phase 5 Event Streaming Consumer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="user-events", help="Kafka topic")
    parser.add_argument("--group-id", default="recommendation-processor", help="Consumer group ID")
    parser.add_argument("--redis-host", default="localhost", help="Redis host")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis port")
    parser.add_argument("--artifacts-dir", help="Path to Phase 4 artifacts")
    parser.add_argument("--max-events", type=int, help="Max events to process (for testing)")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    try:
        # Initialize consumer
        artifacts_dir = Path(args.artifacts_dir) if args.artifacts_dir else None
        consumer = EventStreamingConsumer(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            group_id=args.group_id,
            artifacts_dir=artifacts_dir,
        )
        
        # Initialize Redis
        consumer.initialize_redis(redis_host=args.redis_host, redis_port=args.redis_port)
        
        # Verify artifacts
        if not consumer.verify_artifacts():
            logger.error("Phase 4 artifacts verification failed")
            sys.exit(1)
        
        # Start consuming
        consumer.start_consuming(max_events=args.max_events)
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
