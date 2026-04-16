from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Venue:
    name: str
    address: str
    city: str
    lat: float
    lon: float
    capacity: Optional[int]
    venue_type: str
    source: str
    website: Optional[str] = None
    phone: Optional[str] = None
    rating: Optional[float] = None
    price_per_day: Optional[str] = None
    amenities: list[str] = field(default_factory=list)
    relevance_score: float = 0.0
