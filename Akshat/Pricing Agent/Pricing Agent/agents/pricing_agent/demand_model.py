"""
Demand Model — estimates price-demand relationship using log-linear elasticity.

Concepts:
  - Price elasticity of demand: how quantity demanded changes with price
  - Log-linear model: ln(Q) = a + b * ln(P)  →  Q = A * P^b
  - Elasticity coefficient b < 0 (higher price → lower demand)
  - Category-specific elasticity (tech = inelastic, music = elastic)
"""

import math
from typing import List, Tuple

from agents.pricing_agent.schemas import EventContext, HistoricalEvent
from config import CATEGORY_PROFILES, DEFAULT_CATEGORY_PROFILE, GEOGRAPHY_PPP


class DemandModel:
    """Estimates demand curves and optimal pricing."""

    def __init__(self):
        pass

    def get_category_profile(self, category: str) -> dict:
        """Get pricing profile for event category."""
        # Try exact match first, then case-insensitive
        if category in CATEGORY_PROFILES:
            return CATEGORY_PROFILES[category]

        for key, profile in CATEGORY_PROFILES.items():
            if key.lower() == category.lower():
                return profile

        return DEFAULT_CATEGORY_PROFILE

    def compute_base_price(
        self,
        ctx: EventContext,
        benchmarks: List[HistoricalEvent],
    ) -> float:
        """
        Compute an optimal base ticket price using:
          1. Historical benchmark average
          2. Category profile range
          3. Geography PPP adjustment
          4. Audience size scaling
        """
        profile = self.get_category_profile(ctx.category)
        price_low, price_high = profile["base_price_range"]

        # ── Benchmark-based estimate ────────────────────────────────────
        if benchmarks:
            # Weighted average: more recent events weigh more
            total_weight = 0.0
            weighted_price = 0.0
            for event in benchmarks:
                weight = 1.0 + (event.year - 2023) * 0.3  # newer = higher weight
                weighted_price += event.ticket_price_usd * weight
                total_weight += weight

            benchmark_price = weighted_price / total_weight if total_weight > 0 else 0
        else:
            benchmark_price = (price_low + price_high) / 2

        # ── Category range constraint ───────────────────────────────────
        base_price = max(price_low, min(price_high, benchmark_price))

        # ── Geography PPP adjustment ───────────────────────────────────
        ppp_factor = GEOGRAPHY_PPP.get(ctx.geography, 0.8)
        base_price *= ppp_factor

        # ── Audience size scaling ───────────────────────────────────────
        # Larger events can charge less per ticket (volume economics)
        if ctx.target_audience_size > 10000:
            base_price *= 0.85
        elif ctx.target_audience_size > 5000:
            base_price *= 0.92
        elif ctx.target_audience_size < 500:
            base_price *= 1.20  # exclusive/small events can charge premium

        return round(base_price, 2)

    def estimate_demand(
        self,
        price: float,
        base_price: float,
        base_demand: int,
        elasticity: float,
    ) -> int:
        """
        Estimate quantity demanded at a given price using log-linear model.

        Q = Q_base * (P / P_base) ^ elasticity

        Args:
            price: the price to evaluate
            base_price: reference price point
            base_demand: demand at the reference price
            elasticity: price elasticity (negative number)

        Returns:
            Estimated demand (ticket sales) at the given price
        """
        if price <= 0 or base_price <= 0:
            return base_demand

        ratio = price / base_price
        demand = base_demand * math.pow(ratio, elasticity)
        return max(1, int(round(demand)))

    def compute_conversion_rate(
        self,
        price: float,
        base_price: float,
        base_rate: float,
        elasticity: float,
    ) -> float:
        """
        Estimate conversion rate (probability of purchase) at a given price.
        Uses a logistic-style adjustment around the base rate.

        Returns:
            Conversion probability [0.01, 0.95]
        """
        if price <= 0 or base_price <= 0:
            return base_rate

        # Price deviation from base
        deviation = (price - base_price) / base_price

        # Sigmoid-adjusted conversion: lower price → higher conversion
        adjusted_rate = base_rate * math.exp(-deviation * abs(elasticity))

        # Clamp to reasonable range
        return max(0.01, min(0.95, adjusted_rate))

    def find_optimal_price(
        self,
        base_price: float,
        base_demand: int,
        elasticity: float,
        price_range: Tuple[float, float] = None,
        steps: int = 50,
    ) -> Tuple[float, int, float]:
        """
        Find the price that maximizes revenue within a range.

        Returns:
            (optimal_price, optimal_demand, optimal_revenue)
        """
        if price_range is None:
            price_range = (base_price * 0.3, base_price * 2.5)

        low, high = price_range
        step_size = (high - low) / steps

        best_price = base_price
        best_revenue = 0.0
        best_demand = 0

        for i in range(steps + 1):
            price = low + i * step_size
            demand = self.estimate_demand(price, base_price, base_demand, elasticity)
            revenue = price * demand

            if revenue > best_revenue:
                best_revenue = revenue
                best_price = price
                best_demand = demand

        return round(best_price, 2), best_demand, round(best_revenue, 2)
