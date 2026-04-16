"""
Unit tests for the Pricing & Footfall Agent.
Tests core computation modules without requiring LLM API calls.
"""

import sys
import os
import math

# Ensure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agents.pricing_agent.schemas import (
    EventContext,
    SharedAgentContext,
    TicketTier,
    FootfallPrediction,
    CostBreakdown,
    RevenueProjection,
    HistoricalEvent,
)
from agents.pricing_agent.demand_model import DemandModel
from agents.pricing_agent.footfall_predictor import FootfallPredictor


def test_demand_model_base_price():
    """Test that base price is computed and PPP-adjusted."""
    model = DemandModel()
    ctx = EventContext(
        category="AI",
        geography="India",
        target_audience_size=5000,
    )
    benchmarks = [
        HistoricalEvent(
            event_name="Test AI Conf",
            category="AI",
            geography="India",
            year=2025,
            audience_size=5000,
            ticket_price_usd=120.0,
            attendance=3500,
        ),
    ]
    price = model.compute_base_price(ctx, benchmarks)
    assert price > 0, f"Base price should be positive, got {price}"
    # India PPP factor is 0.45, so price should be < $100
    assert price < 200, f"PPP-adjusted price for India should be < $200, got {price}"
    print(f"  [PASS] Base price for AI/India: ${price:.2f}")


def test_demand_model_elasticity():
    """Test that demand decreases with higher price."""
    model = DemandModel()
    base_demand = 1000
    base_price = 100.0
    elasticity = -1.0

    demand_at_same = model.estimate_demand(100.0, base_price, base_demand, elasticity)
    demand_at_higher = model.estimate_demand(150.0, base_price, base_demand, elasticity)
    demand_at_lower = model.estimate_demand(70.0, base_price, base_demand, elasticity)

    assert demand_at_same == base_demand, f"Same price should give same demand"
    assert demand_at_higher < base_demand, f"Higher price should reduce demand"
    assert demand_at_lower > base_demand, f"Lower price should increase demand"
    print(f"  [PASS] Demand at $100={demand_at_same}, $150={demand_at_higher}, $70={demand_at_lower}")


def test_demand_model_optimal_price():
    """Test revenue maximization."""
    model = DemandModel()
    opt_price, opt_demand, opt_revenue = model.find_optimal_price(
        base_price=100.0,
        base_demand=1000,
        elasticity=-1.0,
        price_range=(30, 300),
    )
    assert opt_price > 0, f"Optimal price should be positive"
    assert opt_demand > 0, f"Optimal demand should be positive"
    assert opt_revenue > 0, f"Optimal revenue should be positive"
    print(f"  [PASS] Optimal: price=${opt_price:.2f}, demand={opt_demand}, revenue=${opt_revenue:,.2f}")


def test_conversion_rate():
    """Test conversion rate decreases with price."""
    model = DemandModel()
    rate_base = model.compute_conversion_rate(100, 100, 0.10, -1.0)
    rate_high = model.compute_conversion_rate(200, 100, 0.10, -1.0)
    rate_low = model.compute_conversion_rate(50, 100, 0.10, -1.0)

    assert rate_high < rate_base, "Higher price should lower conversion"
    assert rate_low > rate_base, "Lower price should raise conversion"
    assert 0.01 <= rate_base <= 0.95, "Conversion rate should be in [0.01, 0.95]"
    print(f"  [PASS] Conversion at $100={rate_base:.4f}, $200={rate_high:.4f}, $50={rate_low:.4f}")


def test_footfall_predictor():
    """Test footfall prediction with sample data."""
    predictor = FootfallPredictor()
    ctx = EventContext(category="AI", geography="India", target_audience_size=5000)
    shared = SharedAgentContext(
        total_community_reach=40000,
        speakers_found=15,
        keynote_count=3,
        sponsors_found=8,
        venue_capacity=5500,
        channels_identified=6,
    )
    tiers = [
        TicketTier("Regular", "Full access", 45.0, 3780.0, "INR", 60, 300, 13500.0, 0.10),
        TicketTier("Workshop Only", "Workshops", 27.0, 2268.0, "INR", 20, 120, 3240.0, 0.12),
        TicketTier("Student", "Student", 15.75, 1323.0, "INR", 20, 150, 2362.5, 0.15),
    ]
    result = predictor.predict(ctx, shared, tiers)

    assert result.expected_attendance > 0, "Should predict positive attendance"
    assert result.lower_bound < result.expected_attendance < result.upper_bound, \
        "Attendance should be within confidence interval"
    assert len(result.attendance_by_tier) == 3, "Should have 3 tier breakdown"
    print(f"  [PASS] Footfall: {result.expected_attendance} "
          f"[{result.lower_bound}-{result.upper_bound}]")
    for name, count in result.attendance_by_tier.items():
        print(f"         {name}: {count}")


def test_shared_context_defaults():
    """Test SharedAgentContext works with missing fields."""
    shared = SharedAgentContext.from_dict({})
    assert shared.sponsors_found == 0
    assert shared.venue_capacity == 0
    assert shared.total_community_reach == 0

    shared2 = SharedAgentContext.from_dict({
        "sponsors_found": 5,
        "unknown_field": "ignored",
    })
    assert shared2.sponsors_found == 5
    assert shared2.venue_capacity == 0
    print(f"  [PASS] SharedAgentContext defaults work correctly")


def test_category_profiles():
    """Test that all expected categories have profiles."""
    model = DemandModel()
    for cat in ["AI", "Web3", "ClimateTech", "Music Festival", "Sports"]:
        profile = model.get_category_profile(cat)
        assert "base_price_range" in profile, f"Missing price range for {cat}"
        assert "demand_elasticity" in profile, f"Missing elasticity for {cat}"
        assert profile["demand_elasticity"] < 0, f"Elasticity should be negative for {cat}"
    print(f"  [PASS] All 5 category profiles valid")


def test_schemas_serialization():
    """Test that all dataclass to_dict methods work."""
    tier = TicketTier("Regular", "Full access", 99.99, 8399.16, "INR", 60, 500, 49995.0, 0.10)
    d = tier.to_dict()
    assert d["price_usd"] == 99.99
    assert d["tier_name"] == "Regular"

    event = HistoricalEvent("Test", "AI", "India", 2025, 5000, 100.0, 3500)
    d = event.to_dict()
    assert d["year"] == 2025

    costs = CostBreakdown(50000, 25000, 10000, 12750, 97750)
    d = costs.to_dict()
    assert d["total_cost"] == 97750.0

    print(f"  [PASS] All schema serialization works")


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("  PRICING AGENT — Unit Tests")
    print("=" * 60)

    tests = [
        test_demand_model_base_price,
        test_demand_model_elasticity,
        test_demand_model_optimal_price,
        test_conversion_rate,
        test_footfall_predictor,
        test_shared_context_defaults,
        test_category_profiles,
        test_schemas_serialization,
    ]

    passed = 0
    failed = 0

    for test_fn in tests:
        try:
            print(f"\n  Running: {test_fn.__name__}")
            test_fn()
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {test_fn.__name__}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n{'='*60}")
    print(f"  Results: {passed} passed, {failed} failed, {passed+failed} total")
    print(f"{'='*60}\n")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
