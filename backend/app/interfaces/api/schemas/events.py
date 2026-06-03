from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional
from datetime import datetime
import uuid


class TrackEventRequest(BaseModel):
    """Request schema for tracking user events."""
    
    user_id: int = Field(..., gt=0, description="User ID")
    movie_id: int = Field(..., gt=0, description="Movie ID")
    event_type: str = Field(..., description="Event type: view, rate, bookmark, share")
    rating: Optional[int] = Field(None, ge=1, le=5, description="Rating (1-5) if event_type is 'rate'")
    timestamp: Optional[int] = Field(None, description="Unix timestamp; server fills if missing")
    
    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v):
        """Validate event_type is one of allowed values."""
        allowed = {'view', 'rate', 'bookmark', 'share'}
        if v not in allowed:
            raise ValueError(f"event_type must be one of {allowed}, got {v}")
        return v
    
    @model_validator(mode="after")
    def validate_rating_with_event_type(self):
        """Validate rating is provided when event_type is 'rate'."""
        if self.event_type == 'rate' and self.rating is None:
            raise ValueError("rating must be provided when event_type is 'rate'")
        if self.event_type != 'rate' and self.rating is not None:
            raise ValueError("rating should not be provided when event_type is not 'rate'")
        return self


class EventAckResponse(BaseModel):
    """Response schema for event acknowledgement."""
    
    event_id: str = Field(..., description="Unique event ID for idempotence tracking")
    status: str = Field(default="accepted", description="Status of the event")
    timestamp: int = Field(..., description="Server timestamp when event was accepted")


def generate_event_id() -> str:
    """Generate a unique event ID."""
    return str(uuid.uuid4())


def get_current_timestamp() -> int:
    """Get current Unix timestamp."""
    return int(datetime.utcnow().timestamp())
