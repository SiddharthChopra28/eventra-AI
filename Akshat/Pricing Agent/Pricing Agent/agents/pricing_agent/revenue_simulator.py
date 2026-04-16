"""
Revenue Simulator — computes full P&L, break-even, and sensitivity analysis.

Aggregates:
  - Ticket revenue (from PricingEngine tiers)
  - Sponsorship revenue (from Sponsor Agent)
  - Exhibitor revenue (from Exhibitor Agent)
  - Costs: venue + speakers + marketing + ops overhead
  - Break-even analysis
  - ±20% demand sensitivity scenarios
  - LLM-generated pricing rationale
"""

import json
import math
from typing import Any, Dict, List, Optional

from google import genai

from config import (
    GEMINI_API_KEY,
    GEMINI_MODEL,
    DEFAULT_OPS_OVERHEAD_PCT,
    DEFAULT_MARKETING_PCT,
)
from agents.pricing_agent.schemas import (
    EventContext,
    SharedAgentContext,
    TicketTier,
    FootfallPrediction,
    CostBreakdown,
    RevenueProjection,
    ScenarioResult,
)


class RevenueSimulator:
    """Computes revenue projections, break-even, and sensitivity analysis."""

    def __init__(self):
        self.client = genai.Client(api_key=GEMINI_API_KEY)

    def simulate(
        self,
        ctx: EventContext,
        shared: SharedAgentContext,
        tiers: List[TicketTier],
        footfall: FootfallPrediction,
    ) -> Dict[str, Any]:
        """
        Run full revenue simulation.

        Returns dict with:
          - revenue_projection: RevenueProjection
          - sensitivity: list of ScenarioResult
          - pricing_rationale: str (LLM-generated)
        """
        # ── Revenue ─────────────────────────────────────────────────────
        ticket_revenue = sum(t.revenue_usd for t in tiers)
        sponsorship_revenue = shared.estimated_sponsorship_revenue
        exhibitor_revenue = shared.estimated_booth_revenue
        total_revenue = ticket_revenue + sponsorship_revenue + exhibitor_revenue

        # ── Costs ───────────────────────────────────────────────────────
        costs = self._compute_costs(ctx, shared, total_revenue)

        # ── Profit ──────────────────────────────────────────────────────
        net_profit = total_revenue - costs.total_cost
        roi_pct = (net_profit / costs.total_cost * 100) if costs.total_cost > 0 else 0.0

        # ── Break-even ──────────────────────────────────────────────────
        break_even = self._compute_break_even(
            tiers, costs.total_cost, sponsorship_revenue, exhibitor_revenue
        )

        revenue_projection = RevenueProjection(
            ticket_revenue=ticket_revenue,
            sponsorship_revenue=sponsorship_revenue,
            exhibitor_revenue=exhibitor_revenue,
            total_revenue=total_revenue,
            costs=costs,
            net_profit=net_profit,
            break_even_attendance=break_even,
            roi_percentage=roi_pct,
        )

        # ── Sensitivity ─────────────────────────────────────────────────
        scenarios = self._run_sensitivity(
            tiers, footfall, sponsorship_revenue, exhibitor_revenue, costs.total_cost
        )

        # ── LLM rationale ──────────────────────────────────────────────
        rationale = self._generate_rationale(ctx, shared, tiers, revenue_projection)

        return {
            "revenue_projection": revenue_projection,
            "sensitivity": scenarios,
            "pricing_rationale": rationale,
        }

    def _compute_costs(
        self,
        ctx: EventContext,
        shared: SharedAgentContext,
        total_revenue: float,
    ) -> CostBreakdown:
        """Build cost breakdown from available data."""
        venue_cost = shared.venue_cost
        speaker_fees = shared.total_speaker_fees

        # If no venue cost from agent, estimate based on budget/audience
        if venue_cost == 0:
            if ctx.budget_max:
                venue_cost = ctx.budget_max * 0.35
            else:
                venue_cost = ctx.target_audience_size * 5.0  # ~$5/head rough estimate

        # If no speaker fees, estimate
        if speaker_fees == 0:
            speaker_fees = max(5000, ctx.target_audience_size * 2.0)

        marketing_cost = total_revenue * DEFAULT_MARKETING_PCT
        ops_overhead = (venue_cost + speaker_fees + marketing_cost) * DEFAULT_OPS_OVERHEAD_PCT

        total = venue_cost + speaker_fees + marketing_cost + ops_overhead

        return CostBreakdown(
            venue_cost=venue_cost,
            speaker_fees=speaker_fees,
            marketing_cost=marketing_cost,
            ops_overhead=ops_overhead,
            total_cost=total,
        )

    def _compute_break_even(
        self,
        tiers: List[TicketTier],
        total_costs: float,
        sponsorship_revenue: float,
        exhibitor_revenue: float,
    ) -> int:
        """
        Compute minimum attendance needed to break even.
        Assumes attendees buy across tiers in the current allocation ratio.
        """
        # Revenue per attendee (weighted average ticket price)
        total_expected = sum(t.expected_sales for t in tiers)
        if total_expected == 0:
            return 0

        avg_ticket_price = sum(t.price_usd * t.expected_sales for t in tiers) / total_expected

        # Revenue needed from tickets to cover costs after other revenue
        ticket_revenue_needed = total_costs - sponsorship_revenue - exhibitor_revenue

        if ticket_revenue_needed <= 0:
            return 0  # Already covered by sponsors + exhibitors

        if avg_ticket_price <= 0:
            return total_expected

        break_even = int(math.ceil(ticket_revenue_needed / avg_ticket_price))
        return max(0, break_even)

    def _run_sensitivity(
        self,
        tiers: List[TicketTier],
        footfall: FootfallPrediction,
        sponsorship: float,
        exhibitor: float,
        total_costs: float,
    ) -> List[ScenarioResult]:
        """Run ±20% and ±40% demand scenarios."""
        scenarios = []
        multipliers = [
            ("Pessimistic (-40%)", 0.6),
            ("Low demand (-20%)", 0.8),
            ("Base case", 1.0),
            ("High demand (+20%)", 1.2),
            ("Optimistic (+40%)", 1.4),
        ]

        for name, mult in multipliers:
            attendance = int(footfall.expected_attendance * mult)
            # Scale ticket revenue proportionally
            base_ticket = sum(t.revenue_usd for t in tiers)
            ticket_rev = base_ticket * mult
            total_rev = ticket_rev + sponsorship + exhibitor
            net = total_rev - total_costs

            scenarios.append(ScenarioResult(
                scenario_name=name,
                demand_multiplier=mult,
                expected_attendance=attendance,
                ticket_revenue=ticket_rev,
                total_revenue=total_rev,
                net_profit=net,
            ))

        return scenarios

    def _generate_rationale(
        self,
        ctx: EventContext,
        shared: SharedAgentContext,
        tiers: List[TicketTier],
        projection: RevenueProjection,
    ) -> str:
        """Use Gemini to generate a human-readable pricing rationale."""
        tier_info = "\n".join([
            f"- {t.tier_name}: ${t.price_usd:.2f} USD "
            f"({t.price_local:.2f} {t.currency}), "
            f"{t.expected_sales} expected sales, "
            f"${t.revenue_usd:,.2f} revenue"
            for t in tiers
        ])

        prompt = f"""Write a concise, professional pricing rationale (3-5 paragraphs) for this conference.

Event: {ctx.category} conference in {ctx.geography}
Target audience: {ctx.target_audience_size:,}
Speakers: {shared.speakers_found} ({shared.keynote_count} keynotes)
Sponsors: {shared.sponsors_found}
Venue: {shared.venue_name or 'TBD'} (capacity: {shared.venue_capacity or 'N/A'})

Ticket Tiers:
{tier_info}

Financial Summary:
- Total ticket revenue: ${projection.ticket_revenue:,.2f}
- Sponsorship revenue: ${projection.sponsorship_revenue:,.2f}
- Exhibitor revenue: ${projection.exhibitor_revenue:,.2f}
- Total revenue: ${projection.total_revenue:,.2f}
- Total costs: ${projection.costs.total_cost:,.2f}
- Net profit: ${projection.net_profit:,.2f}
- Break-even attendance: {projection.break_even_attendance}
- ROI: {projection.roi_percentage:.1f}%

Explain:
1. Why these price points were chosen for the {ctx.geography} market
2. How the tier structure caters to different attendee segments
3. The financial viability of the event
4. Key risks and recommendations

Keep it professional and data-driven. No markdown formatting."""

        try:
            response = self.client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
            )
            return response.text.strip()
        except Exception as e:
            print(f"   [WARN] Rationale generation failed: {e}")
            return (
                f"Pricing set based on {ctx.category} market analysis for {ctx.geography}. "
                f"Regular tier at ${tiers[0].price_usd:.2f} with break-even at "
                f"{projection.break_even_attendance} attendees."
            )


