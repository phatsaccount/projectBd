"""Kafka producer for publishing events."""

import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    KafkaProducer = None
    KafkaError = None

logger = logging.getLogger(__name__)


class EventProducer:
    """Producer for publishing user events to Kafka."""
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "user-events",
        timeout_ms: int = 5000,
    ):
        """Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Kafka broker address(es)
            topic: Kafka topic name for events
            timeout_ms: Producer timeout in milliseconds
        """
        if KafkaProducer is None:
            raise RuntimeError("kafka-python package not installed")
        
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.timeout_seconds = max(timeout_ms / 1000, 1)
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=timeout_ms,
            acks="all",
            retries=3,
        )
        logger.info(f"EventProducer initialized: topic={topic}, servers={bootstrap_servers}")
    
    def publish_event(self, event_data: Dict[str, Any], event_id: str) -> bool:
        """Publish an event to Kafka.
        
        Args:
            event_data: Event data dictionary
            event_id: Unique event ID for tracking
            
        Returns:
            True if published successfully, False otherwise
        """
        try:
            # Add metadata
            payload = {
                **event_data,
                "event_id": event_id,
                "published_at": int(datetime.utcnow().timestamp()),
            }
            
            # Publish and wait for broker acknowledgement. This keeps the API
            # response honest for demos: "accepted" means Kafka actually stored
            # the event, not just that it was queued locally.
            future = self.producer.send(self.topic, value=payload)
            record_metadata = future.get(timeout=self.timeout_seconds)

            logger.info(
                "Event published: event_id=%s, user_id=%s, topic=%s, partition=%s, offset=%s",
                event_id,
                event_data.get("user_id"),
                record_metadata.topic,
                record_metadata.partition,
                record_metadata.offset,
            )
            return True
        except Exception as e:
            logger.error(f"Failed to publish event {event_id}: {str(e)}")
            return False
    
    def _on_send_success(self, record_metadata):
        """Callback on successful publish."""
        logger.debug(
            f"Message sent to partition {record_metadata.partition} "
            f"at offset {record_metadata.offset}"
        )
    
    def _on_send_error(self, exc):
        """Callback on publish error."""
        logger.error(f"Error sending message: {exc}")
    
    def flush(self, timeout_ms: int = 30000) -> None:
        """Flush all pending messages.
        
        Args:
            timeout_ms: Timeout in milliseconds
        """
        self.producer.flush(timeout_ms // 1000)
    
    def close(self) -> None:
        """Close the producer."""
        if self.producer:
            self.producer.close()
        logger.info("EventProducer closed")
