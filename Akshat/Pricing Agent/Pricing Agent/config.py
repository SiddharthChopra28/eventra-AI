"""
Configuration module for the Pricing & Footfall Agent.
Loads environment variables and defines project-wide settings.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── LLM Config ──────────────────────────────────────────────────────────────
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = "gemini-2.0-flash"

# ── Paths ───────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
SEED_DIR = os.path.join(DATA_DIR, "seed")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")

for d in [DATA_DIR, SEED_DIR, OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

# ── Fixed Currency Conversion (to USD) ──────────────────────────────────────
# Rates: 1 USD = X local currency
CURRENCY_TABLE = {
    "India":     {"code": "INR", "rate": 84.0},
    "USA":       {"code": "USD", "rate": 1.0},
    "Europe":    {"code": "EUR", "rate": 0.92},
    "Singapore": {"code": "SGD", "rate": 1.35},
    "UK":        {"code": "GBP", "rate": 0.79},
}

# ── Ticket Tier Definitions ─────────────────────────────────────────────────
# User-confirmed tiers: Regular, Workshop Only, Student
TICKET_TIERS = [
    {
        "name": "Regular",
        "description": "Full conference access (all sessions, networking, keynotes)",
        "allocation_pct": 60,      # % of total tickets allocated to this tier
        "base_multiplier": 1.0,    # price multiplier relative to base price
    },
    {
        "name": "Workshop Only",
        "description": "Access to hands-on workshop sessions only",
        "allocation_pct": 20,
        "base_multiplier": 0.6,
    },
    {
        "name": "Student",
        "description": "Discounted access for students with valid ID",
        "allocation_pct": 20,
        "base_multiplier": 0.35,
    },
]

# ── Category Pricing Profiles ───────────────────────────────────────────────
# Base price ranges (USD) and demand elasticity by event category
CATEGORY_PROFILES = {
    "AI": {
        "base_price_range": (80, 250),
        "demand_elasticity": -0.8,   # inelastic (tech professionals pay)
        "avg_conversion_rate": 0.10,
    },
    "Web3": {
        "base_price_range": (60, 200),
        "demand_elasticity": -1.0,
        "avg_conversion_rate": 0.08,
    },
    "ClimateTech": {
        "base_price_range": (50, 180),
        "demand_elasticity": -0.9,
        "avg_conversion_rate": 0.09,
    },
    "Music Festival": {
        "base_price_range": (30, 150),
        "demand_elasticity": -1.5,   # elastic (price-sensitive audience)
        "avg_conversion_rate": 0.15,
    },
    "Sports": {
        "base_price_range": (25, 120),
        "demand_elasticity": -1.3,
        "avg_conversion_rate": 0.12,
    },
}

# Fallback for unknown categories
DEFAULT_CATEGORY_PROFILE = {
    "base_price_range": (50, 200),
    "demand_elasticity": -1.0,
    "avg_conversion_rate": 0.10,
}

# ── Geography PPP Adjustments ───────────────────────────────────────────────
# Purchasing Power Parity factor: multiply USD prices by this
GEOGRAPHY_PPP = {
    "India":     0.45,   # prices ~45% of US level
    "USA":       1.0,
    "Europe":    0.90,
    "Singapore": 0.85,
    "UK":        0.95,
}

# ── Operational Defaults ────────────────────────────────────────────────────
DEFAULT_OPS_OVERHEAD_PCT = 0.15    # 15% of total costs for misc operations
DEFAULT_MARKETING_PCT = 0.10       # 10% of budget for marketing
