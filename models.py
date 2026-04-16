"""
models.py - Pydantic data models for the Exhibitor Agent
"""
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from enum import Enum


class ExhibitorType(str, Enum):
    STARTUP = "Startup"
    ENTERPRISE = "Enterprise"
    TOOL = "Tool"
    PLATFORM = "Platform"
    OTHERS = "Others"


class RecommendationRequest(BaseModel):
    category: str = Field(..., description="Event category (e.g., AI, FinTech, Cloud)")
    geography: str = Field(..., description="Target country or region")
    audience_size: int = Field(..., ge=100, le=100000, description="Expected audience size")
    top_n: Optional[int] = Field(10, ge=1, le=50, description="Number of recommendations to return")
    min_score: Optional[float] = Field(0.0, ge=0.0, le=100.0, description="Minimum relevance score threshold")

    class Config:
        json_schema_extra = {
            "example": {
                "category": "AI",
                "geography": "India",
                "audience_size": 3000,
                "top_n": 10,
                "min_score": 0.0
            }
        }


class ExhibitorRecommendation(BaseModel):
    name: str
    type: ExhibitorType
    score: float = Field(..., ge=0.0, le=100.0)
    frequency_score: float
    category_match_score: float
    geography_match_score: float
    audience_fit_score: float
    reason: str
    appeared_in_events: List[str]
    appeared_count: int


class ClusterInfo(BaseModel):
    exhibitors: List[str]
    count: int
    percentage: float
    top_exhibitor: Optional[str]


class SimilarEvent(BaseModel):
    event_id: str
    event_name: str
    category: str
    location: str
    audience_size: int
    similarity_score: float
    year: int


class RecommendationResponse(BaseModel):
    query: RecommendationRequest
    similar_events_used: List[SimilarEvent]
    recommended_exhibitors: List[ExhibitorRecommendation]
    clusters: Dict[str, ClusterInfo]
    insights: List[str]
    metadata: Dict[str, Any]


class HealthResponse(BaseModel):
    status: str
    version: str
    dataset_loaded: bool
    total_events: int
    total_unique_exhibitors: int