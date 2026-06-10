"""Event tracking API routes."""

from fastapi import APIRouter, HTTPException, Depends
import logging
import os

from backend.app.interfaces.api.schemas.events import (
    TrackEventRequest,
    EventAckResponse,
    generate_event_id,
    get_current_timestamp,
)
from backend.app.infrastructure.kafka.producer import EventProducer

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/events", tags=["events"])

# Global producer instance (initialized in main.py)
_producer: EventProducer = None
_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def get_producer() -> EventProducer:
    """Dependency: get the event producer."""
    global _producer
    if _producer is None:
        try:
            initialize_producer(bootstrap_servers=_bootstrap_servers)
        except Exception as exc:
            logger.error("Event producer is not ready: %s", exc)
            raise HTTPException(
                status_code=503,
                detail="Kafka producer is not ready. Check Kafka and retry.",
            )
    return _producer


def initialize_producer(bootstrap_servers: str = "localhost:9092") -> EventProducer:
    """Initialize the global event producer."""
    global _producer, _bootstrap_servers
    _bootstrap_servers = bootstrap_servers
    _producer = EventProducer(bootstrap_servers=bootstrap_servers, topic="user-events")
    return _producer


@router.post("/track", response_model=EventAckResponse, status_code=202)
def track_event(
    request: TrackEventRequest,
    producer: EventProducer = Depends(get_producer),
) -> EventAckResponse:
    """Track a user event.
    
    Accepts user interactions (views, ratings, bookmarks, shares) and publishes
    them to Kafka for streaming processing. Returns immediately with event ID
    for idempotence tracking.
    
    Args:
        request: Event data (user_id, movie_id, event_type, optional rating/timestamp)
        producer: Kafka producer instance
        
    Returns:
        EventAckResponse with event_id, status, and server timestamp
        
    Raises:
        HTTPException: If event validation fails or publishing fails
    """
    try:
        # Generate unique event ID
        event_id = generate_event_id()
        
        # Use provided timestamp or server timestamp
        timestamp = request.timestamp or get_current_timestamp()
        
        # Prepare event data for Kafka
        event_data = {
            "user_id": request.user_id,
            "movie_id": request.movie_id,
            "event_type": request.event_type,
            "timestamp": timestamp,
        }
        
        # Add optional rating if provided
        if request.rating is not None:
            event_data["rating"] = request.rating
        
        # Publish to Kafka
        success = producer.publish_event(event_data, event_id)
        
        if not success:
            logger.error(f"Failed to publish event {event_id}")
            raise HTTPException(
                status_code=500,
                detail="Failed to publish event to Kafka"
            )
        
        # Return acknowledgement
        return EventAckResponse(
            event_id=event_id,
            status="accepted",
            timestamp=get_current_timestamp(),
        )
        
    except ValueError as e:
        logger.warning(f"Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in track_event: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
