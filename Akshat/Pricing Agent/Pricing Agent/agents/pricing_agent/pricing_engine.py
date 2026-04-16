"""
Pricing Engine — computes multi-tier ticket pricing.

Takes the base price from DemandModel and generates pricing for each tier:
  - Regular (full access)
  - Workshop Only (workshop sessions)
  - Student (discounted)

Uses tier multipliers from config and demand model for per-tier optimization.
"""

import json
from typing import Any, Dict, List

from google import genai

from config import (
    GEMINI_API_KEY,
    GEMINI_MODEL,
    TICKET_TIERS,
    CURRENCY_TABLE,
    CATEGORY_PROFILES,
    DEFAULT_CATEGORY_PROFILE,
    GEOGRAPHY_PPP,
)
from agents.pricing_agent.schemas import EventContext, SharedAgentContext, TicketTier, HistoricalEvent
from agents.pricing_agent.demand_model import DemandModel


class PricingEngine:
    """Computes multi-tier ticket pricing with demand optimization."""

    def __init__(self):
        self.demand_model = DemandModel()
        self.client = genai.Client(api_key=GEMINI_API_KEY)

    def compute_tiers(
        self,
        ctx: EventContext,
        shared: SharedAgentContext,
        benchmarks: List[HistoricalEvent],
    ) -> List[TicketTier]:
        """
        Compute pricing for all ticket tiers.

        Pipeline:
          1. Compute base price from demand model
          2. Apply tier multipliers
          3. Compute per-tier demand/conversion
          4. Convert to local currency
          5. Validate with LLM
        """
        profile = self.demand_model.get_category_profile(ctx.category)
        elasticity = profile["demand_elasticity"]
        base_conv_rate = profile["avg_conversion_rate"]

        # ── Step 1: Base price ──────────────────────────────────────────
        base_price = self.demand_model.compute_base_price(ctx, benchmarks)
        print(f"   Base price (PPP-adjusted): ${base_price:.2f}")

        # ── Step 2: Effective capacity ──────────────────────────────────
        # Use venue capacity if available, otherwise target audience
        effective_capacity = shared.venue_capacity or ctx.target_audience_size

        # Adjust base conversion if community reach is known
        if shared.total_community_reach > 0:
            reach_ratio = shared.total_community_reach / max(ctx.target_audience_size, 1)
            # Higher reach → higher conversion (capped at 2x base)
            reach_boost = min(2.0, 1.0 + reach_ratio * 0.05)
            base_conv_rate *= reach_boost

        # Speaker quality boosts demand
        if shared.keynote_count >= 3:
            base_conv_rate *= 1.15
        elif shared.speakers_found >= 10:
            base_conv_rate *= 1.08

        # ── Step 3: Per-tier pricing ────────────────────────────────────
        currency_info = CURRENCY_TABLE.get(ctx.geography, {"code": "USD", "rate": 1.0})
        tiers = []

        for tier_config in TICKET_TIERS:
            tier_price_usd = round(base_price * tier_config["base_multiplier"], 2)

            # Per-tier demand
            allocation = tier_config["allocation_pct"] / 100.0
            tier_capacity = int(effective_capacity * allocation)

            # Conversion rate for this tier's price
            conv_rate = self.demand_model.compute_conversion_rate(
                price=tier_price_usd,
                base_price=base_price,
                base_rate=base_conv_rate,
                elasticity=elasticity,
            )

            expected_sales = min(
                tier_capacity,
                int(ctx.target_audience_size * allocation * conv_rate),
            )
            expected_sales = max(1, expected_sales)

            revenue_usd = round(tier_price_usd * expected_sales, 2)
            price_local = round(tier_price_usd * currency_info["rate"], 2)

            tiers.append(TicketTier(
                tier_name=tier_config["name"],
                description=tier_config["description"],
                price_usd=tier_price_usd,
                price_local=price_local,
                currency=currency_info["code"],
                allocation_pct=tier_config["allocation_pct"],
                expected_sales=expected_sales,
                revenue_usd=revenue_usd,
                conversion_rate=round(conv_rate, 4),
            ))

        # ── Step 4: LLM sanity check ───────────────────────────────────
        tiers = self._llm_validate(tiers, ctx, shared)

        return tiers

    def _llm_validate(
        self,
        tiers: List[TicketTier],
        ctx: EventContext,
        shared: SharedAgentContext,
    ) -> List[TicketTier]:
        """Use Gemini to validate and optionally adjust pricing."""
        tier_summary = "\n".join([
            f"- {t.tier_name}: ${t.price_usd:.2f} USD ({t.price_local:.2f} {t.currency}), "
            f"expected {t.expected_sales} sales"
            for t in tiers
        ])

        prompt = f"""You are a conference pricing expert. Review these ticket prices for reasonableness.

Event: {ctx.category} conference in {ctx.geography}
Target audience: {ctx.target_audience_size}
Venue capacity: {shared.venue_capacity or 'unknown'}
Speakers: {shared.speakers_found} (keynotes: {shared.keynote_count})

Proposed pricing:
{tier_summary}

Are these prices reasonable for this type of event in this geography?
If any price seems off, suggest a corrected value.

Return ONLY a JSON object with this exact format, no markdown:
{{
  "is_reasonable": true/false,
  "adjustments": [
    {{"tier_name": "...", "suggested_price_usd": ..., "reason": "..."}}
  ],
  "overall_comment": "brief assessment"
}}

If prices are fine, return empty adjustments array."""

        try:
            response = self.client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
            )
            text = response.text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1]
                text = text.rsplit("```", 1)[0].strip()

            result = json.loads(text)

            if result.get("adjustments"):
                currency_info = CURRENCY_TABLE.get(ctx.geography, {"code": "USD", "rate": 1.0})
                for adj in result["adjustments"]:
                    for tier in tiers:
                        if tier.tier_name == adj["tier_name"]:
                            old_price = tier.price_usd
                            tier.price_usd = round(adj["suggested_price_usd"], 2)
                            tier.price_local = round(tier.price_usd * currency_info["rate"], 2)
                            tier.revenue_usd = round(tier.price_usd * tier.expected_sales, 2)
                            print(f"   [LLM] Adjusted {tier.tier_name}: "
                                  f"${old_price:.2f} → ${tier.price_usd:.2f} "
                                  f"({adj['reason']})")

            comment = result.get("overall_comment", "")
            if comment:
                print(f"   [LLM] Assessment: {comment}")

        except Exception as e:
            print(f"   [WARN] LLM validation skipped: {e}")

        return tiers
