from pydantic import BaseModel, Field
from typing import List, Optional


class RecommendationItem(BaseModel):
    """Single recommendation item."""
    
    movie_id: int = Field(..., description="Movie ID")
    score: float = Field(..., ge=0, le=1, description="Recommendation score (0-1)")
    rank: int = Field(..., description="Rank position (1-indexed)")
    reason: str = Field(..., description="Reason: collaborative, content_based, or popularity")


class RecommendationResponse(BaseModel):
    """Response schema for recommendations endpoint."""
    
    user_id: int = Field(..., description="User ID")
    recommendations: List[RecommendationItem] = Field(default_factory=list, description="Recommended movies")
    total: int = Field(..., description="Total number of recommendations")
    cached: bool = Field(..., description="Whether result was from cache")
    timestamp: int = Field(..., description="Server timestamp")
    computed_at: Optional[int] = Field(None, description="Cache computation timestamp when available")


class RecommendationErrorResponse(BaseModel):
    """Error response schema."""
    
    error: str = Field(..., description="Error message")
    user_id: Optional[int] = Field(None, description="User ID if applicable")
    timestamp: int = Field(..., description="Server timestamp")
