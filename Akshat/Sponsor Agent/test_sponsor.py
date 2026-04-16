"""Quick non-interactive test of the Sponsor Agent."""
import json, sys
sys.path.insert(0, ".")

from agents.sponsor_agent.agent import SponsorAgent

agent = SponsorAgent()
result = agent.run({
    "category": "AI",
    "geography": "India",
    "target_audience_size": 5000,
    "theme_keywords": ["generative AI", "LLMs", "MLOps"],
})

print("\n\n=== FINAL OUTPUT ===")
print(f"Status: {result['status']}")
print(f"Agent: {result['agent_name']}")

if result["status"] == "completed":
    r = result["results"]
    print(f"Total sponsors found: {r['total_sponsors_found']}")
    print(f"\nTop Sponsors:")
    for i, s in enumerate(r["top_sponsors"][:5], 1):
        print(f"  {i}. {s['company_name']} -- Score: {s['relevance_score']:.3f} -- Tier: {s['suggested_tier']}")
        print(f"     Rationale: {s['rationale']}")
    print(f"\nStrategy Summary:\n{r.get('strategy_summary', 'N/A')[:500]}")
else:
    print(f"Error: {result['results']}")
