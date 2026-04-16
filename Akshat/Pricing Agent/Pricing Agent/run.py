"""
CLI Runner — interactive entry point for the Pricing & Footfall Agent.

Usage:
    python run.py
    python run.py --with-context sample_context.json
"""

import json
import sys
import os

from agents.pricing_agent.agent import PricingAgent


def get_user_input() -> dict:
    """Collect event parameters from the user interactively."""
    print("\n" + "=" * 60)
    print("  [*] AI Conference Organizer -- Multi-Agent System")
    print("  ------------------------------------------------")
    print("  Active Agent: Pricing & Footfall Agent")
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


def load_shared_context(filepath: str) -> dict:
    """Load shared context from other agents' output file."""
    if not os.path.exists(filepath):
        print(f"  [WARN] Context file not found: {filepath}")
        return {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"  [WARN] Could not load context: {e}")
        return {}


def display_results(output: dict):
    """Pretty-print the pricing agent results."""
    if output["status"] == "error":
        print(f"\n[ERROR] {output['results'].get('error', 'Unknown error')}")
        return

    results = output["results"]

    print("\n" + "=" * 60)
    print("  PRICING & FOOTFALL AGENT RESULTS")
    print("=" * 60)

    # ── Ticket Tiers ────────────────────────────────────────────────────
    print(f"\n  TICKET TIERS:")
    print(f"  {'─'*55}")
    for tier in results["ticket_tiers"]:
        print(f"\n  {tier['tier_name']}:")
        print(f"    Price:           ${tier['price_usd']:.2f} USD "
              f"({tier['price_local']:.2f} {tier['currency']})")
        print(f"    Allocation:      {tier['allocation_pct']}%")
        print(f"    Expected Sales:  {tier['expected_sales']}")
        print(f"    Revenue:         ${tier['revenue_usd']:,.2f}")
        print(f"    Conversion Rate: {tier['conversion_rate']:.2%}")

    # ── Footfall ────────────────────────────────────────────────────────
    footfall = results["footfall_prediction"]
    print(f"\n  ATTENDANCE FORECAST:")
    print(f"  {'─'*55}")
    print(f"    Expected:  {footfall['expected_attendance']}")
    print(f"    Range:     [{footfall['confidence_interval'][0]} – "
          f"{footfall['confidence_interval'][1]}]")
    if footfall.get("attendance_by_tier"):
        for tier_name, count in footfall["attendance_by_tier"].items():
            print(f"    {tier_name}: {count}")

    # ── Revenue ─────────────────────────────────────────────────────────
    rev = results["revenue_projection"]
    print(f"\n  REVENUE PROJECTION:")
    print(f"  {'─'*55}")
    print(f"    Ticket Revenue:      ${rev['ticket_revenue']:>12,.2f}")
    print(f"    Sponsorship Revenue: ${rev['sponsorship_revenue']:>12,.2f}")
    print(f"    Exhibitor Revenue:   ${rev['exhibitor_revenue']:>12,.2f}")
    print(f"    ────────────────────────────────")
    print(f"    Total Revenue:       ${rev['total_revenue']:>12,.2f}")
    print(f"\n    COSTS:")
    costs = rev["costs"]
    print(f"      Venue:             ${costs['venue_cost']:>12,.2f}")
    print(f"      Speaker Fees:      ${costs['speaker_fees']:>12,.2f}")
    print(f"      Marketing:         ${costs['marketing_cost']:>12,.2f}")
    print(f"      Ops Overhead:      ${costs['ops_overhead']:>12,.2f}")
    print(f"      ──────────────────────────────")
    print(f"      Total Costs:       ${costs['total_cost']:>12,.2f}")
    print(f"\n    Net Profit:          ${rev['net_profit']:>12,.2f}")
    print(f"    ROI:                 {rev['roi_percentage']:.1f}%")
    print(f"    Break-even:          {rev['break_even_attendance']} attendees")

    # ── Sensitivity ─────────────────────────────────────────────────────
    print(f"\n  SENSITIVITY ANALYSIS:")
    print(f"  {'─'*55}")
    print(f"  {'Scenario':<25} {'Attendance':>10} {'Revenue':>14} {'Profit':>14}")
    print(f"  {'─'*55}")
    for s in results["sensitivity_analysis"]:
        profit_str = f"${s['net_profit']:>11,.2f}"
        if s["net_profit"] < 0:
            profit_str = f"-${abs(s['net_profit']):>10,.2f}"
        print(f"  {s['scenario']:<25} {s['attendance']:>10,} "
              f"${s['total_revenue']:>11,.2f} {profit_str}")

    # ── Rationale ───────────────────────────────────────────────────────
    if results.get("pricing_rationale"):
        print(f"\n  PRICING RATIONALE:")
        print(f"  {'─'*55}")
        for line in results["pricing_rationale"].split("\n"):
            print(f"  {line}")

    # ── Market Comparison ───────────────────────────────────────────────
    mc = results.get("market_comparison", {})
    if mc:
        print(f"\n  MARKET BENCHMARKS:")
        print(f"  {'─'*55}")
        print(f"    Benchmark avg price:      ${mc.get('benchmark_avg_price', 0):.2f}")
        print(f"    Benchmark avg attendance: {mc.get('benchmark_avg_attendance', 0)}")
        print(f"    Benchmarks used:          {results.get('benchmarks_used', 0)}")

    print(f"\n{'='*60}")
    print(f"  [SAVED] Full results saved to data/output/ directory")
    print(f"{'='*60}\n")


def main():
    """Main entry point."""
    event_context = get_user_input()

    # Check for shared context file
    shared_context = {}
    if "--with-context" in sys.argv:
        idx = sys.argv.index("--with-context")
        if idx + 1 < len(sys.argv):
            shared_context = load_shared_context(sys.argv[idx + 1])
            print(f"\n  [LOADED] Shared context from {sys.argv[idx+1]}")

    # Ask user if they want to simulate other agents' outputs
    if not shared_context:
        simulate = input(
            "\n[?] Load sample shared context from other agents? (y/N): "
        ).strip().lower()
        if simulate == "y":
            sample_file = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "data", "seed", "sample_shared_context.json"
            )
            shared_context = load_shared_context(sample_file)
            if shared_context:
                print(f"  [LOADED] Sample shared context")
            else:
                print("  [INFO] No sample context found, running standalone")

    event_context["shared_context"] = shared_context

    print("\n" + "-" * 60)
    print("  Running with:")
    for k, v in event_context.items():
        if v is not None and k != "shared_context":
            print(f"    {k}: {v}")
    if shared_context:
        print(f"    shared_context: {len(shared_context)} fields from other agents")
    print("-" * 60)

    # Run pricing agent
    agent = PricingAgent()
    output = agent.run(event_context)

    # Display results
    display_results(output)


if __name__ == "__main__":
    main()
