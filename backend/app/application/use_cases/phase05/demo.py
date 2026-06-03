"""End-to-end demo script for Phase 5."""

import requests
import time
import json
import sys
import logging
from typing import Dict, List, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class Phase5Demo:
    """Demo for Phase 5: Streaming Events and Recommendations."""
    
    def __init__(
        self,
        api_base_url: str = "http://localhost:8000",
        demo_user_id: int = 1,
        demo_movies: List[int] = None,
    ):
        """Initialize demo.
        
        Args:
            api_base_url: Base URL for the API
            demo_user_id: User ID for demo
            demo_movies: List of movie IDs to interact with
        """
        self.api_base_url = api_base_url
        self.demo_user_id = demo_user_id
        self.demo_movies = demo_movies or [1, 2, 3, 4, 5, 10, 20, 30]
        self.event_ids: List[str] = []
    
    def check_api_health(self) -> bool:
        """Check if API is healthy.
        
        Returns:
            True if API is running and healthy
        """
        try:
            response = requests.get(f"{self.api_base_url}/health", timeout=5)
            if response.status_code == 200:
                logger.info("✓ API health check passed")
                return True
            else:
                logger.error(f"✗ API health check failed: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"✗ API health check failed: {e}")
            return False
    
    def get_initial_recommendations(self) -> Dict[str, Any]:
        """Get initial recommendations before any events.
        
        Returns:
            Recommendations response
        """
        try:
            response = requests.get(
                f"{self.api_base_url}/recommendations/{self.demo_user_id}",
                params={"k": 5},
                timeout=10,
            )
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✓ Got initial recommendations (cached={data.get('cached', False)})")
                logger.info(f"  Recommendations: {[r['movie_id'] for r in data.get('recommendations', [])]}")
                return data
            else:
                logger.error(f"✗ Failed to get recommendations: {response.status_code}")
                return {}
        except Exception as e:
            logger.error(f"✗ Failed to get recommendations: {e}")
            return {}
    
    def track_user_events(self) -> bool:
        """Track user events.
        
        Returns:
            True if all events tracked successfully
        """
        logger.info(f"Tracking {len(self.demo_movies)} events for user {self.demo_user_id}...")
        
        all_success = True
        for idx, movie_id in enumerate(self.demo_movies):
            try:
                # Alternate between view and rate events
                event_type = "rate" if idx % 2 == 0 else "view"
                rating = 5 if event_type == "rate" else None
                
                payload = {
                    "user_id": self.demo_user_id,
                    "movie_id": movie_id,
                    "event_type": event_type,
                }
                
                if rating:
                    payload["rating"] = rating
                
                response = requests.post(
                    f"{self.api_base_url}/events/track",
                    json=payload,
                    timeout=5,
                )
                
                if response.status_code == 202:
                    data = response.json()
                    event_id = data.get("event_id")
                    self.event_ids.append(event_id)
                    logger.info(f"  ✓ Event {idx+1}/{len(self.demo_movies)}: {event_type} movie {movie_id}")
                else:
                    logger.error(f"  ✗ Failed to track event: {response.status_code}")
                    all_success = False
                
                # Small delay between events
                time.sleep(0.1)
            
            except Exception as e:
                logger.error(f"  ✗ Error tracking event: {e}")
                all_success = False
        
        if all_success:
            logger.info(f"✓ All {len(self.event_ids)} events tracked successfully")
        return all_success
    
    def wait_for_processing(self, wait_time: int = 5) -> None:
        """Wait for streaming consumer to process events.
        
        Args:
            wait_time: Time to wait in seconds
        """
        logger.info(f"Waiting {wait_time}s for streaming consumer to process events...")
        for i in range(wait_time, 0, -1):
            print(f"  {i}s remaining...", end="\r")
            time.sleep(1)
        print(" " * 30, end="\r")  # Clear line
    
    def get_final_recommendations(self) -> Dict[str, Any]:
        """Get recommendations after events.
        
        Returns:
            Recommendations response
        """
        try:
            response = requests.get(
                f"{self.api_base_url}/recommendations/{self.demo_user_id}",
                params={"k": 5},
                timeout=10,
            )
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✓ Got final recommendations (cached={data.get('cached', False)})")
                logger.info(f"  Recommendations: {[r['movie_id'] for r in data.get('recommendations', [])]}")
                return data
            else:
                logger.error(f"✗ Failed to get recommendations: {response.status_code}")
                return {}
        except Exception as e:
            logger.error(f"✗ Failed to get recommendations: {e}")
            return {}
    
    def compare_recommendations(self, initial: Dict, final: Dict) -> bool:
        """Compare initial and final recommendations.
        
        Args:
            initial: Initial recommendations
            final: Final recommendations
            
        Returns:
            True if recommendations changed
        """
        initial_ids = [r["movie_id"] for r in initial.get("recommendations", [])]
        final_ids = [r["movie_id"] for r in final.get("recommendations", [])]
        
        if initial_ids == final_ids:
            logger.warning("! Recommendations did not change (may be normal if cache not updated yet)")
            return False
        else:
            logger.info("✓ Recommendations changed after events!")
            logger.info(f"  Initial: {initial_ids}")
            logger.info(f"  Final:   {final_ids}")
            return True
    
    def run(self) -> bool:
        """Run the full demo.
        
        Returns:
            True if demo completed successfully
        """
        logger.info("=" * 60)
        logger.info("Phase 5 - Streaming Events and Recommendation Serving Demo")
        logger.info("=" * 60)
        
        # Step 1: Check API
        if not self.check_api_health():
            return False
        
        # Step 2: Get initial recommendations
        initial = self.get_initial_recommendations()
        if not initial:
            logger.warning("Could not get initial recommendations, continuing anyway...")
        
        # Step 3: Track events
        if not self.track_user_events():
            logger.error("Failed to track events")
            return False
        
        # Step 4: Wait for processing
        self.wait_for_processing(wait_time=5)
        
        # Step 5: Get final recommendations
        final = self.get_final_recommendations()
        if not final:
            logger.error("Failed to get final recommendations")
            return False
        
        # Step 6: Compare
        changed = self.compare_recommendations(initial, final)
        
        logger.info("=" * 60)
        if changed:
            logger.info("✓ Demo completed successfully!")
            logger.info("  Events were tracked and recommendations were updated.")
        else:
            logger.info("Demo completed (recommendations may not have changed).")
        logger.info("=" * 60)
        
        return True


def main():
    """Run the demo."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Phase 5 Demo")
    parser.add_argument("--api-url", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--user-id", type=int, default=1, help="Demo user ID")
    parser.add_argument("--num-events", type=int, default=8, help="Number of events to track")
    
    args = parser.parse_args()
    
    # Create demo
    demo = Phase5Demo(
        api_base_url=args.api_url,
        demo_user_id=args.user_id,
        demo_movies=list(range(1, args.num_events + 1)),
    )
    
    # Run
    success = demo.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
