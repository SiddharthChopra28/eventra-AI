"""
Data models for the Sponsor Agent.
Plain dataclasses — no ORM dependency needed at this stage.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class PastSponsorship:
    """A single historical sponsorship record."""
    event_name: str
    event_category: str          # AI, Web3, ClimateTech, Music, Sports …
    year: int
    tier: str                    # Title, Gold, Silver, Bronze, Partner, …
    geography: str               # Country or region
    event_url: Optional[str] = None


@dataclass
class Sponsor:
    """A potential sponsor entity."""
    company_name: str
    industry: str                # e.g. "Cloud Computing", "FinTech"
    headquarters: str            # geography — "India", "USA", …
    company_size: str            # "startup", "mid", "enterprise"
    description: str
    website: Optional[str] = None
    past_sponsorships: List[PastSponsorship] = field(default_factory=list)
    marketing_focus: List[str] = field(default_factory=list)

    # ── Computed at ranking time ────────────────────────────────────────
    relevance_score: float = 0.0
    scoring_breakdown: dict = field(default_factory=dict)
    suggested_tier: str = ""
    estimated_value: str = ""
    rationale: str = ""
    proposal: str = ""

    def to_dict(self) -> dict:
        return {
            "company_name": self.company_name,
            "industry": self.industry,
            "headquarters": self.headquarters,
            "company_size": self.company_size,
            "description": self.description,
            "website": self.website,
            "past_sponsorships": [
                {
                    "event_name": s.event_name,
                    "event_category": s.event_category,
                    "year": s.year,
                    "tier": s.tier,
                    "geography": s.geography,
                }
                for s in self.past_sponsorships
            ],
            "marketing_focus": self.marketing_focus,
            "relevance_score": round(self.relevance_score, 3),
            "scoring_breakdown": {
                k: round(v, 3) for k, v in self.scoring_breakdown.items()
            },
            "suggested_tier": self.suggested_tier,
            "estimated_value": self.estimated_value,
            "rationale": self.rationale,
            "proposal": self.proposal,
        }


@dataclass
class EventContext:
    """The input that every agent receives from the orchestrator."""
    category: str                         # "AI", "Web3", "Music Festival", …
    geography: str                        # "India", "USA", "Europe", …
    target_audience_size: int
    theme_keywords: List[str] = field(default_factory=list)
    budget_min: Optional[float] = None
    budget_max: Optional[float] = None
