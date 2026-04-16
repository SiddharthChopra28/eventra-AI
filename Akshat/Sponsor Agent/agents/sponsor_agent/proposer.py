"""
Sponsor Proposal Generator -- uses Gemini to create personalised sponsorship proposals.

Two modes:
  1. Batch enrichment: Enrich scraped sponsor profiles with industry/description via LLM
  2. Proposal generation: Generate custom sponsorship proposal text for top sponsors
"""

import json
import time
from typing import Any, Dict, List

from google import genai

from config import GEMINI_API_KEY, GEMINI_MODEL
from agents.sponsor_agent.schemas import EventContext, Sponsor

# Retry config for rate-limited APIs
MAX_LLM_RETRIES = 3
RETRY_BASE_DELAY = 10  # seconds


def _call_gemini_with_retry(client, model: str, prompt: str) -> str:
    """Call Gemini with automatic retry on rate-limit (429) errors."""
    for attempt in range(MAX_LLM_RETRIES):
        try:
            response = client.models.generate_content(
                model=model,
                contents=prompt,
            )
            return response.text.strip()
        except Exception as e:
            error_str = str(e)
            if "limit: 0" in error_str:
                print(f"   [RATE-LIMIT] Hard quota limit reached (limit: 0). Falling back to mock data.")
                # Return a JSON array string that can be parsed for the enrichment
                # or plain text for proposals depending on the prompt
                if "JSON array" in prompt:
                    return '[{"company_name": "Unknown", "industry": "Technology", "company_size": "mid", "headquarters": "Global", "description": "Tech company", "marketing_focus": ["tech"]}]'
                else:
                    return "This is a mock proposal. The Gemini API key provided has exceeded its quota or is disabled in your region. Please add a billing account."
            elif "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                delay = RETRY_BASE_DELAY * (attempt + 1)
                print(f"   [RATE-LIMIT] Hit rate limit, retrying in {delay}s (attempt {attempt+1}/{MAX_LLM_RETRIES})...")
                time.sleep(delay)
            else:
                raise
    
    print("   [ERROR] Max retries exceeded for Gemini API rate limit. Falling back.")
    if "JSON array" in prompt:
        return '[]'
    return "[Proposal generation failed over rate limit]"


class ProposalGenerator:
    """Generates sponsorship proposals using Gemini."""

    def __init__(self):
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self.model = GEMINI_MODEL

    # -- Batch Enrichment ------------------------------------------------

    def enrich_sponsors(self, sponsors: List[Sponsor]) -> List[Sponsor]:
        """
        Enrich sponsor objects with industry, company size, and description
        by sending company names to Gemini in batches.
        """
        # Process in batches of 60 to stay within token limits and reduce API calls
        batch_size = 60

        for i in range(0, len(sponsors), batch_size):
            batch = sponsors[i : i + batch_size]
            names = [s.company_name for s in batch]

            prompt = f"""You are a business analyst. For each company below, provide:
1. industry (e.g. "Cloud Computing", "FinTech", "Consumer Electronics")
2. company_size: one of "startup", "mid", "enterprise"
3. headquarters: primary country/region (e.g. "USA", "India", "Europe")
4. description: one-sentence description of what the company does
5. marketing_focus: list of 2-3 keywords describing their marketing focus areas

Companies:
{json.dumps(names)}

Respond ONLY with a JSON array of objects. Each object MUST have these exact keys:
"company_name", "industry", "company_size", "headquarters", "description", "marketing_focus"

If you don't know a company, make your best guess based on the name.
Do NOT include any text before or after the JSON array."""

            try:
                text = _call_gemini_with_retry(self.client, self.model, prompt)

                # Clean up markdown code fences if present
                if text.startswith("```"):
                    text = text.split("\n", 1)[1]
                    text = text.rsplit("```", 1)[0]
                    text = text.strip()

                enriched = json.loads(text)

                # Map back to sponsor objects
                enriched_map = {
                    e["company_name"].lower(): e for e in enriched
                }

                for sponsor in batch:
                    key = sponsor.company_name.lower()
                    if key in enriched_map:
                        info = enriched_map[key]
                        sponsor.industry = info.get("industry", "")
                        sponsor.company_size = info.get("company_size", "")
                        sponsor.headquarters = info.get("headquarters", "")
                        sponsor.description = info.get("description", "")
                        sponsor.marketing_focus = info.get("marketing_focus", [])

                print(f"   [OK] Enriched batch {i // batch_size + 1} ({len(batch)} sponsors)")

            except Exception as e:
                print(f"   [WARN] Enrichment failed for batch {i // batch_size + 1}: {e}")

            # Small delay between batches to respect rate limits
            time.sleep(2)

        return sponsors

    # -- Proposal Generation ---------------------------------------------

    def generate_proposals(
        self,
        sponsors: List[Sponsor],
        context: EventContext,
        top_n: int = 10,
    ) -> List[Sponsor]:
        """
        Generate custom sponsorship proposals for the top-ranked sponsors.
        """
        top_sponsors = sponsors[:top_n]

        for idx, sponsor in enumerate(top_sponsors, 1):
            print(f"   Generating proposal {idx}/{len(top_sponsors)}: {sponsor.company_name}...")
            proposal = self._generate_single_proposal(sponsor, context)
            sponsor.proposal = proposal
            time.sleep(2)  # rate limit buffer

        return sponsors

    def _generate_single_proposal(
        self, sponsor: Sponsor, context: EventContext
    ) -> str:
        """Generate a single personalised sponsorship proposal."""

        past_events_str = ""
        if sponsor.past_sponsorships:
            events = [
                f"- {sp.event_name} ({sp.tier}, {sp.year})"
                for sp in sponsor.past_sponsorships[:5]
            ]
            past_events_str = "\n".join(events)

        prompt = f"""You are a professional conference sponsorship manager. Write a compelling, 
personalised sponsorship proposal for the following company to sponsor an upcoming event.

## Company Profile
- Name: {sponsor.company_name}
- Industry: {sponsor.industry}
- Size: {sponsor.company_size}
- Description: {sponsor.description}
- Marketing Focus: {', '.join(sponsor.marketing_focus)}
{f"- Past Sponsorships:\\n{past_events_str}" if past_events_str else ""}

## Event Details
- Category: {context.category}
- Geography: {context.geography}
- Target Audience: {context.target_audience_size:,} attendees
- Themes: {', '.join(context.theme_keywords) if context.theme_keywords else context.category}

## Suggested Tier: {sponsor.suggested_tier}
## Estimated Value: {sponsor.estimated_value}

## Requirements for the proposal:
1. Open with why this company is a perfect fit for THIS specific event
2. Highlight mutual benefits (brand visibility, lead generation, thought leadership)
3. Include specific deliverables for the {sponsor.suggested_tier} tier:
   - Logo placement
   - Speaking slots
   - Booth/exhibition space
   - Digital marketing mentions
   - Networking opportunities
4. Reference their past sponsorship activity if available
5. Keep it professional but compelling, 200-300 words
6. End with a clear call-to-action

Write the proposal as a formal letter/email body. Do NOT include subject line or headers."""

        try:
            return _call_gemini_with_retry(self.client, self.model, prompt)
        except Exception as e:
            return f"[Proposal generation failed: {e}]"

    # -- Utility: Generate event-level summary ---------------------------

    def generate_sponsor_strategy(
        self,
        sponsors: List[Sponsor],
        context: EventContext,
    ) -> str:
        """
        Generate an overall sponsorship strategy summary for the event
        based on all recommended sponsors.
        """
        sponsor_summary = []
        for i, s in enumerate(sponsors[:10], 1):
            sponsor_summary.append(
                f"{i}. {s.company_name} (Score: {s.relevance_score:.2f}, "
                f"Tier: {s.suggested_tier}, Value: {s.estimated_value})"
            )

        prompt = f"""You are a senior event sponsorship strategist. Based on the following 
sponsor recommendations for an upcoming {context.category} event in {context.geography} 
targeting {context.target_audience_size:,} attendees, provide a brief sponsorship strategy.

## Recommended Sponsors:
{chr(10).join(sponsor_summary)}

Write a 150-word strategy summary covering:
1. Overall sponsorship revenue potential
2. Key value propositions for sponsors
3. Recommended outreach priority order
4. One unique sponsorship activation idea

Be concise and actionable."""

        try:
            return _call_gemini_with_retry(self.client, self.model, prompt)
        except Exception as e:
            return f"[Strategy generation failed: {e}]"
