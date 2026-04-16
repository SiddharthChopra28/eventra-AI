"""
Sponsor Agent — the top-level orchestrator for sponsor discovery.

Pipeline:
  1. Scrape real sponsor data from conference websites
  2. Consolidate into Sponsor objects
  3. Enrich with industry/company info via Gemini
  4. Rank by relevance to the target event
  5. Generate personalised proposals for top sponsors
  6. Return structured results
"""

import json
import os
from typing import Any, Dict, List

from agents.base_agent import BaseAgent
from agents.sponsor_agent.schemas import EventContext, Sponsor
from agents.sponsor_agent.scraper import SponsorScraper, build_sponsor_database
from agents.sponsor_agent.ranker import SponsorRanker
from agents.sponsor_agent.proposer import ProposalGenerator
from config import SCRAPED_DIR


class SponsorAgent(BaseAgent):
    """
    AI agent that discovers, ranks, and generates proposals for event sponsors.
    """

    def __init__(self):
        super().__init__(name="sponsor_agent")
        self.scraper = SponsorScraper()
        self.ranker = SponsorRanker()
        self.proposer = ProposalGenerator()

    def run(self, event_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the full sponsor discovery pipeline.

        Args:
            event_context: dict with keys:
                - category (str): e.g. "AI", "Web3", "Music Festival"
                - geography (str): e.g. "India", "USA", "Europe"
                - target_audience_size (int)
                - theme_keywords (list[str], optional)
                - budget_min (float, optional)
                - budget_max (float, optional)
        """
        ctx = EventContext(
            category=event_context["category"],
            geography=event_context["geography"],
            target_audience_size=event_context["target_audience_size"],
            theme_keywords=event_context.get("theme_keywords", []),
            budget_min=event_context.get("budget_min"),
            budget_max=event_context.get("budget_max"),
        )

        print(f"\n{'='*60}")
        print(f"  [AGENT] SPONSOR AGENT -- Starting")
        print(f"  Category: {ctx.category}")
        print(f"  Geography: {ctx.geography}")
        print(f"  Audience: {ctx.target_audience_size:,}")
        print(f"{'='*60}")

        try:
            # ── Step 1: Scrape sponsors ─────────────────────────────────
            print("\n>> Step 1/5: Scraping sponsor data ...")
            raw_data = self._get_or_scrape(ctx)
            print(f"   Found {len(raw_data)} raw sponsor records")

            if not raw_data:
                return self._build_response(
                    status="error",
                    results={"error": "No sponsor data found. Try a different category/geography."},
                )

            # ── Step 2: Build sponsor database ──────────────────────────
            print("\n>> Step 2/5: Building sponsor database ...")
            sponsors = build_sponsor_database(raw_data)
            print(f"   Consolidated into {len(sponsors)} unique sponsors")

            # ── Step 3: Enrich with LLM ─────────────────────────────────
            print("\n>> Step 3/5: Enriching sponsor profiles via Gemini ...")
            sponsors = self.proposer.enrich_sponsors(sponsors)

            # ── Step 4: Rank ────────────────────────────────────────────
            print("\n>> Step 4/5: Ranking sponsors ...")
            sponsors = self.ranker.rank(sponsors, ctx)

            # ── Step 5: Generate proposals for top sponsors ─────────────
            top_n = min(10, len(sponsors))
            print(f"\n>> Step 5/5: Generating proposals for top {top_n} sponsors ...")
            sponsors = self.proposer.generate_proposals(sponsors, ctx, top_n=top_n)

            # ── Generate overall strategy ───────────────────────────────
            print("\n>> Generating sponsorship strategy ...")
            strategy = self.proposer.generate_sponsor_strategy(sponsors, ctx)

            # ── Build output ────────────────────────────────────────────
            results = {
                "total_sponsors_found": len(sponsors),
                "top_sponsors": [s.to_dict() for s in sponsors[:top_n]],
                "all_sponsors": [s.to_dict() for s in sponsors],
                "strategy_summary": strategy,
            }

            # Save results
            self._save_results(results, ctx)

            print(f"\n{'='*60}")
            print(f"  [OK] SPONSOR AGENT -- Complete")
            print(f"  Found {len(sponsors)} sponsors, top {top_n} ranked & proposals generated")
            print(f"{'='*60}\n")

            return self._build_response(
                status="completed",
                results=results,
                context_updates={
                    "sponsors_found": len(sponsors),
                    "top_sponsor_names": [s.company_name for s in sponsors[:5]],
                },
            )

        except Exception as e:
            print(f"\n[ERROR] Sponsor Agent failed: {e}")
            import traceback
            traceback.print_exc()
            return self._build_response(
                status="error",
                results={"error": str(e)},
            )

    # ── Helpers ─────────────────────────────────────────────────────────

    def _get_or_scrape(self, ctx: EventContext) -> List[Dict[str, Any]]:
        """
        Check if we already have scraped data for this category/geography.
        If yes, load it. Otherwise, scrape fresh.
        """
        filename = f"sponsors_{ctx.category.lower()}_{ctx.geography.lower()}.json"
        filepath = os.path.join(SCRAPED_DIR, filename)

        if os.path.exists(filepath):
            print(f"   [CACHE] Loading cached data from {filepath}")
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)

        return self.scraper.scrape_all(
            category=ctx.category,
            geography=ctx.geography,
        )

    def _save_results(self, results: Dict[str, Any], ctx: EventContext):
        """Save final results to a JSON file."""
        filename = f"results_{ctx.category.lower()}_{ctx.geography.lower()}.json"
        filepath = os.path.join(SCRAPED_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"   [SAVED] Results saved to {filepath}")
