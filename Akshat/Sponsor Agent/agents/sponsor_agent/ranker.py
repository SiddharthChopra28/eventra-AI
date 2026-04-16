"""
Sponsor Ranking Engine — scores and ranks sponsors by relevance to the target event.

Scoring formula (weighted multi-criteria):
  relevance_score = w1 * industry_relevance
                  + w2 * geography_fit
                  + w3 * historical_frequency
                  + w4 * category_match
                  + w5 * tier_strength

Weights are configurable. Scores are normalised to [0, 1].
"""

from typing import Dict, List

from agents.sponsor_agent.schemas import EventContext, Sponsor


# ── Default weights (sum to 1.0) ────────────────────────────────────────
DEFAULT_WEIGHTS: Dict[str, float] = {
    "industry_relevance": 0.25,
    "geography_fit": 0.20,
    "historical_frequency": 0.20,
    "category_match": 0.25,
    "tier_strength": 0.10,
}

# ── Category → related industry keywords ────────────────────────────────
CATEGORY_INDUSTRY_MAP: Dict[str, List[str]] = {
    "ai": [
        "artificial intelligence", "machine learning", "ai", "deep learning",
        "data science", "cloud", "software", "technology", "computing",
        "saas", "analytics", "nlp", "robotics", "automation", "gpu",
        "semiconductor", "research", "developer tools",
    ],
    "web3": [
        "blockchain", "crypto", "web3", "defi", "nft", "decentralized",
        "fintech", "cryptocurrency", "dao", "smart contract", "wallet",
        "exchange", "protocol", "layer 2",
    ],
    "climatetech": [
        "climate", "sustainability", "cleantech", "renewable", "energy",
        "carbon", "green", "environment", "ev", "electric", "solar",
        "battery", "recycling",
    ],
    "music festival": [
        "music", "entertainment", "media", "streaming", "audio",
        "beverage", "beer", "lifestyle", "fashion", "telecom",
        "consumer electronics", "retail",
    ],
    "music": [
        "music", "entertainment", "media", "streaming", "audio",
        "beverage", "beer", "lifestyle", "fashion", "telecom",
        "consumer electronics", "retail",
    ],
    "sports": [
        "sports", "fitness", "athletics", "apparel", "beverage",
        "nutrition", "telecom", "media", "broadcasting", "automotive",
        "insurance", "banking",
    ],
}

# ── Tier hierarchy (higher value = stronger signal) ─────────────────────
TIER_SCORES: Dict[str, float] = {
    "title": 1.0,
    "presenting": 1.0,
    "platinum": 0.95,
    "diamond": 0.95,
    "gold": 0.80,
    "silver": 0.60,
    "bronze": 0.40,
    "partner": 0.30,
    "community": 0.20,
    "media": 0.15,
}


class SponsorRanker:
    """Scores and ranks sponsors for a given event context."""

    def __init__(self, weights: Dict[str, float] | None = None):
        self.weights = weights or DEFAULT_WEIGHTS

    def rank(
        self,
        sponsors: List[Sponsor],
        context: EventContext,
    ) -> List[Sponsor]:
        """
        Score every sponsor and return them sorted highest → lowest.
        Each sponsor object is mutated in-place with scoring details.
        """
        for sponsor in sponsors:
            breakdown = self._score(sponsor, context)
            total = sum(
                self.weights[k] * breakdown[k] for k in self.weights
            )
            sponsor.relevance_score = total
            sponsor.scoring_breakdown = breakdown
            sponsor.suggested_tier = self._suggest_tier(total)
            sponsor.estimated_value = self._estimate_value(
                total, context.target_audience_size
            )
            sponsor.rationale = self._build_rationale(sponsor, breakdown, context)

        sponsors.sort(key=lambda s: s.relevance_score, reverse=True)
        return sponsors

    # ── Individual scoring dimensions ───────────────────────────────────

    def _score(self, sponsor: Sponsor, ctx: EventContext) -> Dict[str, float]:
        return {
            "industry_relevance": self._score_industry(sponsor, ctx),
            "geography_fit": self._score_geography(sponsor, ctx),
            "historical_frequency": self._score_frequency(sponsor, ctx),
            "category_match": self._score_category(sponsor, ctx),
            "tier_strength": self._score_tier(sponsor),
        }

    def _score_industry(self, sponsor: Sponsor, ctx: EventContext) -> float:
        """How well the sponsor's industry matches the event category."""
        keywords = CATEGORY_INDUSTRY_MAP.get(ctx.category.lower(), [])
        if not keywords:
            # Fallback: use theme keywords if category not in map
            keywords = [k.lower() for k in ctx.theme_keywords]

        if not keywords:
            return 0.5  # neutral

        industry_lower = sponsor.industry.lower()
        desc_lower = sponsor.description.lower()
        focus_lower = " ".join(sponsor.marketing_focus).lower()
        text = f"{industry_lower} {desc_lower} {focus_lower}"

        matches = sum(1 for kw in keywords if kw in text)
        return min(matches / max(len(keywords) * 0.3, 1), 1.0)

    def _score_geography(self, sponsor: Sponsor, ctx: EventContext) -> float:
        """Score based on geographic alignment."""
        if not sponsor.headquarters:
            # Check past sponsorships for geographic signal
            geo_hits = sum(
                1 for sp in sponsor.past_sponsorships
                if sp.geography.lower() == ctx.geography.lower()
            )
            total = len(sponsor.past_sponsorships) or 1
            return min(geo_hits / total + 0.2, 1.0)

        sponsor_geo = sponsor.headquarters.lower()
        target_geo = ctx.geography.lower()

        if sponsor_geo == target_geo:
            return 1.0
        # Multinational companies often sponsor globally
        if sponsor.company_size == "enterprise":
            return 0.7
        return 0.3

    def _score_frequency(self, sponsor: Sponsor, ctx: EventContext) -> float:
        """More past sponsorships = higher score."""
        n = len(sponsor.past_sponsorships)
        if n == 0:
            return 0.1
        elif n == 1:
            return 0.4
        elif n <= 3:
            return 0.7
        else:
            return 1.0

    def _score_category(self, sponsor: Sponsor, ctx: EventContext) -> float:
        """Score based on how often they've sponsored events in this category."""
        if not sponsor.past_sponsorships:
            return 0.3

        category_hits = sum(
            1 for sp in sponsor.past_sponsorships
            if sp.event_category.lower() == ctx.category.lower()
        )
        total = len(sponsor.past_sponsorships)
        return min(category_hits / total + 0.2, 1.0)

    def _score_tier(self, sponsor: Sponsor) -> float:
        """Higher past sponsorship tiers = stronger sponsor."""
        if not sponsor.past_sponsorships:
            return 0.3

        tier_scores = [
            TIER_SCORES.get(sp.tier.lower(), 0.25)
            for sp in sponsor.past_sponsorships
        ]
        return max(tier_scores)

    # ── Tier suggestion & value estimation ──────────────────────────────

    def _suggest_tier(self, score: float) -> str:
        if score >= 0.85:
            return "Title Sponsor"
        elif score >= 0.70:
            return "Gold Sponsor"
        elif score >= 0.50:
            return "Silver Sponsor"
        else:
            return "Bronze Sponsor"

    def _estimate_value(self, score: float, audience_size: int) -> str:
        """Rough sponsorship value estimation based on score and audience."""
        # Base value scales with audience
        base = audience_size * 5  # ≈ $5 per attendee baseline

        if score >= 0.85:
            low = int(base * 2)
            high = int(base * 5)
        elif score >= 0.70:
            low = int(base * 1)
            high = int(base * 2)
        elif score >= 0.50:
            low = int(base * 0.5)
            high = int(base * 1)
        else:
            low = int(base * 0.2)
            high = int(base * 0.5)

        # Format nicely
        def fmt(v):
            if v >= 1_000_000:
                return f"${v/1_000_000:.1f}M"
            elif v >= 1_000:
                return f"${v/1_000:.0f}K"
            return f"${v}"

        return f"{fmt(low)} - {fmt(high)}"

    def _build_rationale(
        self, sponsor: Sponsor, breakdown: Dict[str, float], ctx: EventContext
    ) -> str:
        """Generate a short human-readable rationale for the ranking."""
        parts = []

        if breakdown["category_match"] >= 0.7:
            n = sum(
                1 for sp in sponsor.past_sponsorships
                if sp.event_category.lower() == ctx.category.lower()
            )
            parts.append(
                f"Sponsored {n} {ctx.category} event(s) before"
            )

        if breakdown["geography_fit"] >= 0.7:
            parts.append(f"Strong presence in {ctx.geography}")

        if breakdown["tier_strength"] >= 0.7:
            best_tier = max(
                sponsor.past_sponsorships,
                key=lambda sp: TIER_SCORES.get(sp.tier.lower(), 0),
            )
            parts.append(
                f"Previously a {best_tier.tier} sponsor at {best_tier.event_name}"
            )

        if breakdown["historical_frequency"] >= 0.7:
            parts.append(
                f"Frequent sponsor ({len(sponsor.past_sponsorships)} events)"
            )

        if not parts:
            parts.append("Potential fit based on industry alignment")

        return "; ".join(parts) + "."
