"""
Pricing Agent — top-level orchestrator for pricing & footfall prediction.

Pipeline:
  1. Collect historical benchmark data (DataCollector)
  2. Compute multi-tier ticket pricing (PricingEngine + DemandModel)
  3. Predict attendance / footfall (FootfallPredictor)
  4. Simulate revenue + break-even + sensitivity (RevenueSimulator)
  5. Return structured results with LLM-generated rationale
"""

import json
import os
from typing import Any, Dict

from agents.base_agent import BaseAgent
from agents.pricing_agent.schemas import EventContext, SharedAgentContext
from agents.pricing_agent.data_collector import DataCollector
from agents.pricing_agent.pricing_engine import PricingEngine
from agents.pricing_agent.footfall_predictor import FootfallPredictor
from agents.pricing_agent.revenue_simulator import RevenueSimulator
from config import OUTPUT_DIR


class PricingAgent(BaseAgent):
    """
    AI agent that predicts ticket pricing, attendance,
    and generates revenue projections for conferences/events.
    """

    def __init__(self):
        super().__init__(name="pricing_agent")
        self.data_collector = DataCollector()
        self.pricing_engine = PricingEngine()
        self.footfall_predictor = FootfallPredictor()
        self.revenue_simulator = RevenueSimulator()

    def run(self, event_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the full pricing pipeline.

        Args:
            event_context: dict with keys:
                - category (str)
                - geography (str)
                - target_audience_size (int)
                - theme_keywords (list[str], optional)
                - budget_min (float, optional)
                - budget_max (float, optional)
                - shared_context (dict, optional): outputs from other agents
        """
        # ── Parse inputs ────────────────────────────────────────────────
        ctx = EventContext(
            category=event_context["category"],
            geography=event_context["geography"],
            target_audience_size=event_context["target_audience_size"],
            theme_keywords=event_context.get("theme_keywords", []),
            budget_min=event_context.get("budget_min"),
            budget_max=event_context.get("budget_max"),
        )

        shared = SharedAgentContext.from_dict(
            event_context.get("shared_context", {})
        )

        print(f"\n{'='*60}")
        print(f"  [AGENT] PRICING & FOOTFALL AGENT -- Starting")
        print(f"  Category: {ctx.category}")
        print(f"  Geography: {ctx.geography}")
        print(f"  Target Audience: {ctx.target_audience_size:,}")
        if shared.venue_name:
            print(f"  Venue: {shared.venue_name} (cap: {shared.venue_capacity})")
        if shared.sponsors_found:
            print(f"  Sponsors: {shared.sponsors_found}")
        if shared.speakers_found:
            print(f"  Speakers: {shared.speakers_found}")
        print(f"{'='*60}")

        try:
            # ── Step 1: Collect benchmark data ──────────────────────────
            print("\n>> Step 1/4: Collecting historical benchmark data ...")
            benchmarks = self.data_collector.collect(ctx)

            # ── Step 2: Compute tier pricing ────────────────────────────
            print("\n>> Step 2/4: Computing multi-tier ticket pricing ...")
            tiers = self.pricing_engine.compute_tiers(ctx, shared, benchmarks)
            for t in tiers:
                print(f"   {t.tier_name}: ${t.price_usd:.2f} USD "
                      f"({t.price_local:.2f} {t.currency}) "
                      f"→ {t.expected_sales} expected sales")

            # ── Step 3: Predict footfall ────────────────────────────────
            print("\n>> Step 3/4: Predicting attendance / footfall ...")
            footfall = self.footfall_predictor.predict(ctx, shared, tiers)
            print(f"   Expected attendance: {footfall.expected_attendance}")
            print(f"   Confidence interval: "
                  f"[{footfall.lower_bound}, {footfall.upper_bound}]")

            # ── Step 4: Revenue simulation ──────────────────────────────
            print("\n>> Step 4/4: Running revenue simulation ...")
            simulation = self.revenue_simulator.simulate(
                ctx, shared, tiers, footfall
            )
            projection = simulation["revenue_projection"]
            scenarios = simulation["sensitivity"]
            rationale = simulation["pricing_rationale"]

            print(f"   Total revenue: ${projection.total_revenue:,.2f}")
            print(f"   Total costs:   ${projection.costs.total_cost:,.2f}")
            print(f"   Net profit:    ${projection.net_profit:,.2f}")
            print(f"   ROI:           {projection.roi_percentage:.1f}%")
            print(f"   Break-even at: {projection.break_even_attendance} attendees")

            # ── Build output ────────────────────────────────────────────
            results = {
                "ticket_tiers": [t.to_dict() for t in tiers],
                "footfall_prediction": footfall.to_dict(),
                "revenue_projection": projection.to_dict(),
                "sensitivity_analysis": [s.to_dict() for s in scenarios],
                "pricing_rationale": rationale,
                "benchmarks_used": len(benchmarks),
                "market_comparison": {
                    "benchmark_avg_price": (
                        round(
                            sum(b.ticket_price_usd for b in benchmarks) / len(benchmarks), 2
                        ) if benchmarks else 0
                    ),
                    "benchmark_avg_attendance": (
                        int(sum(b.attendance for b in benchmarks) / len(benchmarks))
                        if benchmarks else 0
                    ),
                },
            }

            # Save results
            self._save_results(results, ctx)

            print(f"\n{'='*60}")
            print(f"  [OK] PRICING & FOOTFALL AGENT -- Complete")
            print(f"{'='*60}\n")

            return self._build_response(
                status="completed",
                results=results,
                context_updates={
                    "ticket_price_range": [
                        min(t.price_usd for t in tiers),
                        max(t.price_usd for t in tiers),
                    ],
                    "expected_attendance": footfall.expected_attendance,
                    "total_projected_revenue": projection.total_revenue,
                    "break_even_attendance": projection.break_even_attendance,
                },
            )

        except Exception as e:
            print(f"\n[ERROR] Pricing Agent failed: {e}")
            import traceback
            traceback.print_exc()
            return self._build_response(
                status="error",
                results={"error": str(e)},
            )

    def _save_results(self, results: Dict[str, Any], ctx: EventContext):
        """Save final results to a JSON file."""
        filename = f"pricing_{ctx.category.lower()}_{ctx.geography.lower()}.json"
        filepath = os.path.join(OUTPUT_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"   [SAVED] Results saved to {filepath}")
