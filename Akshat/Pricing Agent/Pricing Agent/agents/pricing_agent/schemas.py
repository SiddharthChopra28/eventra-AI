"""
Data models for the Pricing & Footfall Agent.
Plain dataclasses — no ORM dependency needed.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ── Input Models ────────────────────────────────────────────────────────────

@dataclass
class EventContext:
    """The core input that every agent receives from the orchestrator."""
    category: str                         # "AI", "Web3", "Music Festival", …
    geography: str                        # "India", "USA", "Europe", …
    target_audience_size: int
    theme_keywords: List[str] = field(default_factory=list)
    budget_min: Optional[float] = None
    budget_max: Optional[float] = None


@dataclass
class SharedAgentContext:
    """
    Speculated outputs from upstream agents.
    All fields have defaults so PricingAgent works even if run standalone.
    """
    # ── From Sponsor Agent ──────────────────────────────────────────────
    sponsors_found: int = 0
    top_sponsor_names: List[str] = field(default_factory=list)
    estimated_sponsorship_revenue: float = 0.0
    sponsor_tiers: Dict[str, int] = field(default_factory=dict)

    # ── From Speaker Agent ──────────────────────────────────────────────
    speakers_found: int = 0
    total_speaker_fees: float = 0.0
    session_count: int = 0
    keynote_count: int = 0

    # ── From Exhibitor Agent ────────────────────────────────────────────
    exhibitors_found: int = 0
    estimated_booth_revenue: float = 0.0

    # ── From Venue Agent ────────────────────────────────────────────────
    venue_name: str = ""
    venue_capacity: int = 0
    venue_cost: float = 0.0
    venue_city: str = ""

    # ── From Community & GTM Agent ──────────────────────────────────────
    total_community_reach: int = 0
    channels_identified: int = 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SharedAgentContext":
        """Build from a dict, ignoring unknown keys."""
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)


# ── Output Models ───────────────────────────────────────────────────────────

@dataclass
class TicketTier:
    """Pricing for a single ticket tier."""
    tier_name: str
    description: str
    price_usd: float
    price_local: float
    currency: str
    allocation_pct: float         # % of total tickets
    expected_sales: int
    revenue_usd: float
    conversion_rate: float        # probability of purchase

    def to_dict(self) -> dict:
        return {
            "tier_name": self.tier_name,
            "description": self.description,
            "price_usd": round(self.price_usd, 2),
            "price_local": round(self.price_local, 2),
            "currency": self.currency,
            "allocation_pct": round(self.allocation_pct, 1),
            "expected_sales": self.expected_sales,
            "revenue_usd": round(self.revenue_usd, 2),
            "conversion_rate": round(self.conversion_rate, 4),
        }


@dataclass
class FootfallPrediction:
    """Attendance forecast."""
    expected_attendance: int
    lower_bound: int
    upper_bound: int
    attendance_by_tier: Dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "expected_attendance": self.expected_attendance,
            "confidence_interval": [self.lower_bound, self.upper_bound],
            "attendance_by_tier": self.attendance_by_tier,
        }


@dataclass
class CostBreakdown:
    """All costs associated with the event."""
    venue_cost: float
    speaker_fees: float
    marketing_cost: float
    ops_overhead: float
    total_cost: float

    def to_dict(self) -> dict:
        return {
            "venue_cost": round(self.venue_cost, 2),
            "speaker_fees": round(self.speaker_fees, 2),
            "marketing_cost": round(self.marketing_cost, 2),
            "ops_overhead": round(self.ops_overhead, 2),
            "total_cost": round(self.total_cost, 2),
        }


@dataclass
class RevenueProjection:
    """Complete financial projection."""
    ticket_revenue: float
    sponsorship_revenue: float
    exhibitor_revenue: float
    total_revenue: float
    costs: CostBreakdown
    net_profit: float
    break_even_attendance: int
    roi_percentage: float

    def to_dict(self) -> dict:
        return {
            "ticket_revenue": round(self.ticket_revenue, 2),
            "sponsorship_revenue": round(self.sponsorship_revenue, 2),
            "exhibitor_revenue": round(self.exhibitor_revenue, 2),
            "total_revenue": round(self.total_revenue, 2),
            "costs": self.costs.to_dict(),
            "net_profit": round(self.net_profit, 2),
            "break_even_attendance": self.break_even_attendance,
            "roi_percentage": round(self.roi_percentage, 2),
        }


@dataclass
class ScenarioResult:
    """A single what-if scenario."""
    scenario_name: str
    demand_multiplier: float
    expected_attendance: int
    ticket_revenue: float
    total_revenue: float
    net_profit: float

    def to_dict(self) -> dict:
        return {
            "scenario": self.scenario_name,
            "demand_multiplier": self.demand_multiplier,
            "attendance": self.expected_attendance,
            "ticket_revenue": round(self.ticket_revenue, 2),
            "total_revenue": round(self.total_revenue, 2),
            "net_profit": round(self.net_profit, 2),
        }


@dataclass
class HistoricalEvent:
    """A single historical event record used as benchmark."""
    event_name: str
    category: str
    geography: str
    year: int
    audience_size: int
    ticket_price_usd: float
    attendance: int
    sponsorship_revenue: float = 0.0
    exhibitor_revenue: float = 0.0
    speaker_count: int = 0
    venue_cost: float = 0.0

    def to_dict(self) -> dict:
        return {
            "event_name": self.event_name,
            "category": self.category,
            "geography": self.geography,
            "year": self.year,
            "audience_size": self.audience_size,
            "ticket_price_usd": round(self.ticket_price_usd, 2),
            "attendance": self.attendance,
            "sponsorship_revenue": round(self.sponsorship_revenue, 2),
            "exhibitor_revenue": round(self.exhibitor_revenue, 2),
            "speaker_count": self.speaker_count,
            "venue_cost": round(self.venue_cost, 2),
        }
