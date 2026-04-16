from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Artist:
    name: str
    city: str
    genre: str
    source: str
    upcoming_events: Optional[int] = None
    popularity_score: Optional[float] = None
    profile_url: Optional[str] = None
    notes: Optional[str] = None
    tags: list[str] = field(default_factory=list)
    relevance_score: float = 0.0
