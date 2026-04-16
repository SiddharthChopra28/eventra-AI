"""
Configuration module — loads environment variables and defines project-wide settings.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── LLM Config ──────────────────────────────────────────────────────────────
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = "gemini-2.0-flash"   # fast + cheap, good for hackathon

# ── Paths ───────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
SEED_DIR = os.path.join(DATA_DIR, "seed")
SCRAPED_DIR = os.path.join(DATA_DIR, "scraped")

# Ensure directories exist
for d in [DATA_DIR, SEED_DIR, SCRAPED_DIR]:
    os.makedirs(d, exist_ok=True)

# ── Scraper Config ──────────────────────────────────────────────────────────
REQUEST_TIMEOUT = 15          # seconds
REQUEST_DELAY = 1.0           # polite delay between requests (seconds)
MAX_RETRIES = 3
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)
