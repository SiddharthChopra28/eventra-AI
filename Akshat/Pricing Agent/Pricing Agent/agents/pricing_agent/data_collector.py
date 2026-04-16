"""
Data Collector — gathers historical event pricing data for benchmarking.

Uses:
  1. Local seed dataset (if available)
  2. Gemini LLM to generate realistic benchmark data based on real-world conferences
"""

import json
import os
from typing import Any, Dict, List

from google import genai

from config import GEMINI_API_KEY, GEMINI_MODEL, SEED_DIR
from agents.pricing_agent.schemas import EventContext, HistoricalEvent


class DataCollector:
    """Collects and manages historical event pricing data."""

    def __init__(self):
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self.seed_file = os.path.join(SEED_DIR, "historical_events.json")

    def collect(self, ctx: EventContext) -> List[HistoricalEvent]:
        """
        Get historical events for benchmarking.
        Loads seed data first, then enriches with LLM if needed.
        """
        events = self._load_seed_data(ctx)

        if len(events) < 5:
            print("   Enriching with LLM-generated benchmarks ...")
            llm_events = self._generate_benchmarks(ctx)
            events.extend(llm_events)
            self._save_seed_data(events)

        print(f"   {len(events)} benchmark events available")
        return events

    def _load_seed_data(self, ctx: EventContext) -> List[HistoricalEvent]:
        """Load existing seed data, filtered by category."""
        if not os.path.exists(self.seed_file):
            return []

        try:
            with open(self.seed_file, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except (json.JSONDecodeError, IOError):
            return []

        events = []
        for item in raw:
            try:
                events.append(HistoricalEvent(**item))
            except (TypeError, KeyError):
                continue

        # Filter to same category (or return all if too few)
        same_cat = [e for e in events if e.category.lower() == ctx.category.lower()]
        return same_cat if len(same_cat) >= 3 else events

    def _generate_benchmarks(self, ctx: EventContext) -> List[HistoricalEvent]:
        """Use Gemini to generate realistic benchmark data."""
        prompt = f"""Generate 10 realistic historical conference/event records for benchmarking.

Target event profile:
- Category: {ctx.category}
- Geography: {ctx.geography}
- Target audience: {ctx.target_audience_size}

For each event, provide data based on REAL well-known conferences in this domain.
For example, if category is AI, reference events like GTC, NeurIPS, ICML, AI Summit, etc.
If Music Festival, reference Coachella, Tomorrowland, Sunburn, NH7 Weekender, etc.

Return ONLY a JSON array, no markdown, no explanation. Each object must have exactly these fields:
{{
  "event_name": "string (real or realistic event name)",
  "category": "{ctx.category}",
  "geography": "string (country/region)",
  "year": integer (2024 or 2025),
  "audience_size": integer (target audience/capacity),
  "ticket_price_usd": float (average ticket price in USD),
  "attendance": integer (actual attendance),
  "sponsorship_revenue": float (total sponsorship revenue USD),
  "exhibitor_revenue": float (total exhibitor revenue USD),
  "speaker_count": integer,
  "venue_cost": float (venue rental in USD)
}}

Make the data realistic — vary prices by geography and event size.
Include a mix of geographies: {ctx.geography} and similar regions.
Prices should reflect real-world ranges for {ctx.category} events."""

        try:
            response = self.client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
            )
            text = response.text.strip()

            # Strip markdown code fences if present
            if text.startswith("```"):
                text = text.split("\n", 1)[1]
                text = text.rsplit("```", 1)[0].strip()

            raw_events = json.loads(text)

            events = []
            for item in raw_events:
                try:
                    events.append(HistoricalEvent(**item))
                except (TypeError, KeyError) as e:
                    print(f"   Skipping malformed event: {e}")
                    continue

            return events

        except Exception as e:
            print(f"   [WARN] LLM benchmark generation failed: {e}")
            return self._fallback_benchmarks(ctx)

    def _fallback_benchmarks(self, ctx: EventContext) -> List[HistoricalEvent]:
        """Hardcoded fallback if LLM fails — minimal set for basic pricing."""
        return [
            HistoricalEvent(
                event_name=f"Sample {ctx.category} Conference 2025",
                category=ctx.category,
                geography=ctx.geography,
                year=2025,
                audience_size=ctx.target_audience_size,
                ticket_price_usd=100.0,
                attendance=int(ctx.target_audience_size * 0.65),
                sponsorship_revenue=50000.0,
                exhibitor_revenue=20000.0,
                speaker_count=15,
                venue_cost=30000.0,
            ),
        ]

    def _save_seed_data(self, events: List[HistoricalEvent]):
        """Persist enriched data back to seed file."""
        try:
            data = [e.to_dict() for e in events]
            with open(self.seed_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"   [SAVED] Seed data saved to {self.seed_file}")
        except IOError as e:
            print(f"   [WARN] Could not save seed data: {e}")
