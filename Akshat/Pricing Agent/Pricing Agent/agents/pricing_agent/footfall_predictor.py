"""
Footfall Predictor — forecasts expected attendance per tier.

Uses:
  - Pricing tier data (from PricingEngine)
  - Community reach (from GTM Agent)
  - Venue capacity (from Venue Agent)
  - Speaker draw (from Speaker Agent)
  - Sigmoid conversion model
"""

import math
from typing import Dict, List

from agents.pricing_agent.schemas import (
    EventContext,
    SharedAgentContext,
    TicketTier,
    FootfallPrediction,
)
from config import CATEGORY_PROFILES, DEFAULT_CATEGORY_PROFILE


class FootfallPredictor:
    """Predicts total and per-tier attendance."""

    def predict(
        self,
        ctx: EventContext,
        shared: SharedAgentContext,
        tiers: List[TicketTier],
    ) -> FootfallPrediction:
        """
        Compute attendance prediction with confidence interval.

        Methodology:
          1. Sum expected_sales across tiers (from PricingEngine)
          2. Apply correction factors: community reach, speaker quality
          3. Cap at venue capacity
          4. Compute confidence interval (±15% default)
        """
        # ── Base attendance from tier sales ──────────────────────────────
        raw_attendance = sum(t.expected_sales for t in tiers)
        attendance_by_tier: Dict[str, int] = {
            t.tier_name: t.expected_sales for t in tiers
        }

        # ── Correction factors ──────────────────────────────────────────
        factors = 1.0

        # Community reach boost
        if shared.total_community_reach > 0:
            reach_ratio = shared.total_community_reach / max(ctx.target_audience_size, 1)
            # Diminishing returns: lots of reach helps, but plateaus
            community_factor = 1.0 + self._sigmoid(reach_ratio, midpoint=2.0, steepness=1.5) * 0.3
            factors *= community_factor

        # Speaker draw factor
        if shared.speakers_found > 0:
            # Each keynote is worth 2 regular speakers for draw
            effective_speaker_score = shared.speakers_found + shared.keynote_count * 2
            speaker_factor = 1.0 + self._sigmoid(
                effective_speaker_score, midpoint=15, steepness=0.2
            ) * 0.2
            factors *= speaker_factor

        # Sponsor credibility boost (big sponsors attract attendees)
        if shared.sponsors_found >= 5:
            factors *= 1.08
        elif shared.sponsors_found >= 2:
            factors *= 1.04

        # GTM channels factor
        if shared.channels_identified >= 8:
            factors *= 1.10
        elif shared.channels_identified >= 4:
            factors *= 1.05

        adjusted_attendance = int(round(raw_attendance * factors))

        # ── Cap at venue capacity ───────────────────────────────────────
        if shared.venue_capacity > 0:
            adjusted_attendance = min(adjusted_attendance, shared.venue_capacity)

        # ── Redistribute across tiers proportionally ────────────────────
        if raw_attendance > 0:
            scale = adjusted_attendance / raw_attendance
            attendance_by_tier = {
                name: max(1, int(round(count * scale)))
                for name, count in attendance_by_tier.items()
            }
            # Ensure sum matches
            diff = adjusted_attendance - sum(attendance_by_tier.values())
            if diff != 0:
                # Add/remove from the largest tier
                largest_tier = max(attendance_by_tier, key=attendance_by_tier.get)
                attendance_by_tier[largest_tier] += diff

        # ── Confidence interval ─────────────────────────────────────────
        # Higher uncertainty if we have less upstream data
        data_richness = sum([
            1 if shared.total_community_reach > 0 else 0,
            1 if shared.speakers_found > 0 else 0,
            1 if shared.sponsors_found > 0 else 0,
            1 if shared.venue_capacity > 0 else 0,
        ])
        uncertainty = 0.25 - (data_richness * 0.03)  # 25% to 13%
        uncertainty = max(0.10, uncertainty)

        lower_bound = max(1, int(adjusted_attendance * (1 - uncertainty)))
        upper_bound = int(adjusted_attendance * (1 + uncertainty))
        if shared.venue_capacity > 0:
            upper_bound = min(upper_bound, shared.venue_capacity)

        return FootfallPrediction(
            expected_attendance=adjusted_attendance,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            attendance_by_tier=attendance_by_tier,
        )

    @staticmethod
    def _sigmoid(x: float, midpoint: float = 0.0, steepness: float = 1.0) -> float:
        """
        Standard sigmoid function scaled to [0, 1].
        Used for smooth factor curves with diminishing returns.
        """
        z = steepness * (x - midpoint)
        # Clamp to avoid overflow
        z = max(-20.0, min(20.0, z))
        return 1.0 / (1.0 + math.exp(-z))
