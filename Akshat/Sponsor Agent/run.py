"""
CLI Runner — interactive entry point for the multi-agent system.
Currently supports: Sponsor Agent.

Usage:
    python run.py
"""

import json
import sys

from agents.sponsor_agent.agent import SponsorAgent


def get_user_input() -> dict:
    """Collect event parameters from the user interactively."""
    print("\n" + "=" * 60)
    print("  [*] AI Conference Organizer -- Multi-Agent System")
    print("  ---------------------------------------------")
    print("  Currently active: Sponsor Agent")
    print("=" * 60)

    # -- Category --------------------------------------------------------
    categories = ["AI", "Web3", "ClimateTech", "Music Festival", "Sports"]
    print("\n[*] Available event categories:")
    for i, cat in enumerate(categories, 1):
        print(f"   {i}. {cat}")
    print(f"   {len(categories)+1}. Custom (type your own)")

    choice = input("\n  Select category (number or name): ").strip()
    if choice.isdigit() and 1 <= int(choice) <= len(categories):
        category = categories[int(choice) - 1]
    elif choice.isdigit() and int(choice) == len(categories) + 1:
        category = input("  Enter custom category: ").strip()
    else:
        category = choice if choice else "AI"

    # -- Geography -------------------------------------------------------
    geographies = ["India", "USA", "Europe", "Singapore"]
    print("\n[*] Available geographies:")
    for i, geo in enumerate(geographies, 1):
        print(f"   {i}. {geo}")
    print(f"   {len(geographies)+1}. Custom (type your own)")

    choice = input("\n  Select geography (number or name): ").strip()
    if choice.isdigit() and 1 <= int(choice) <= len(geographies):
        geography = geographies[int(choice) - 1]
    elif choice.isdigit() and int(choice) == len(geographies) + 1:
        geography = input("  Enter custom geography: ").strip()
    else:
        geography = choice if choice else "India"

    # -- Audience size ---------------------------------------------------
    audience_str = input("\n[?] Target audience size (default 5000): ").strip()
    try:
        audience_size = int(audience_str)
    except (ValueError, TypeError):
        audience_size = 5000

    # -- Theme keywords --------------------------------------------------
    keywords_str = input(
        "\n[?] Theme keywords (comma-separated, or press Enter to skip): "
    ).strip()
    theme_keywords = [k.strip() for k in keywords_str.split(",") if k.strip()] if keywords_str else []

    # -- Budget ----------------------------------------------------------
    budget_str = input(
        "\n[?] Budget range in USD (e.g. '50000-200000', or press Enter to skip): "
    ).strip()
    budget_min = None
    budget_max = None
    if budget_str and "-" in budget_str:
        parts = budget_str.split("-")
        try:
            budget_min = float(parts[0].strip())
            budget_max = float(parts[1].strip())
        except ValueError:
            pass

    return {
        "category": category,
        "geography": geography,
        "target_audience_size": audience_size,
        "theme_keywords": theme_keywords,
        "budget_min": budget_min,
        "budget_max": budget_max,
    }


def display_results(output: dict):
    """Pretty-print the sponsor agent results."""
    if output["status"] == "error":
        print(f"\n[ERROR] {output['results'].get('error', 'Unknown error')}")
        return

    results = output["results"]

    print("\n" + "=" * 60)
    print("  SPONSOR AGENT RESULTS")
    print("=" * 60)

    print(f"\n  Total sponsors found: {results['total_sponsors_found']}")
    print(f"  Showing top {len(results['top_sponsors'])} recommendations:\n")

    for i, sponsor in enumerate(results["top_sponsors"], 1):
        print(f"  ---{'-' * 53}")
        print(f"  #{i} -- {sponsor['company_name']}")
        print(f"  ---{'-' * 53}")
        print(f"  Relevance Score : {sponsor['relevance_score']:.3f}")
        print(f"  Suggested Tier  : {sponsor['suggested_tier']}")
        print(f"  Est. Value      : {sponsor['estimated_value']}")
        print(f"  Industry        : {sponsor['industry']}")
        print(f"  Headquarters    : {sponsor['headquarters']}")
        print(f"  Company Size    : {sponsor['company_size']}")
        print(f"  Rationale       : {sponsor['rationale']}")

        if sponsor.get("scoring_breakdown"):
            print(f"  Scoring Breakdown:")
            for k, v in sponsor["scoring_breakdown"].items():
                bar = "#" * int(v * 20) + "." * (20 - int(v * 20))
                print(f"    {k:25s}  {bar}  {v:.3f}")

        if sponsor.get("past_sponsorships"):
            print(f"  Past Sponsorships:")
            for sp in sponsor["past_sponsorships"][:3]:
                print(f"    - {sp['event_name']} ({sp['tier']}, {sp['year']})")

        if sponsor.get("proposal") and sponsor["proposal"]:
            print(f"\n  Proposal Preview:")
            # Show first 200 chars of proposal
            preview = sponsor["proposal"][:300]
            for line in preview.split("\n"):
                print(f"     {line}")
            if len(sponsor["proposal"]) > 300:
                print(f"     ... (truncated, see full output JSON)")
        print()

    # Strategy summary
    if results.get("strategy_summary"):
        print(f"\n{'='*60}")
        print(f"  SPONSORSHIP STRATEGY")
        print(f"{'='*60}")
        for line in results["strategy_summary"].split("\n"):
            print(f"  {line}")

    print(f"\n{'='*60}")
    print(f"  [SAVED] Full results saved to data/scraped/ directory")
    print(f"{'='*60}\n")


def main():
    """Main entry point."""
    event_context = get_user_input()

    print("\n" + "-" * 60)
    print("  Running with:")
    for k, v in event_context.items():
        if v is not None:
            print(f"    {k}: {v}")
    print("-" * 60)

    # Run sponsor agent
    agent = SponsorAgent()
    output = agent.run(event_context)

    # Display results
    display_results(output)


if __name__ == "__main__":
    main()
