"""
================================================================================
EVENT AGGREGATION & ENRICHMENT PIPELINE
================================================================================
A production-grade pipeline to scrape, aggregate, enrich, and export a
comprehensive database of past and upcoming music/live events.

INSTALLATION:
    pip install aiohttp asyncio playwright extruct curl_cffi pydantic \
                litellm instructor pandas tqdm fake_useragent lxml \
                cssselect beautifulsoup4 python-dateutil tenacity loguru

    playwright install chromium

ENVIRONMENT VARIABLES (create a .env file or export before running):
    BANDSINTOWN_APP_ID       -> Your Bandsintown app_id (register at https://artists.bandsintown.com/support/bandsintown-api)
    EVENTBRITE_TOKEN         -> Your Eventbrite private token (https://www.eventbrite.com/platform/api)
    GEMINI_API_KEY           -> Your Google Gemini API key for LLM enrichment
    LLM_PROVIDER             -> "gemini" (default: gemini)

USAGE:
    python event_pipeline.py

OUTPUT FILES:
    events_raw.json              -> Intermediate checkpoint (Phases 1 combined)
    events_enriched.json         -> Intermediate checkpoint (Phase 2 output)
    events_master_database.csv   -> Final output (Phase 3)
================================================================================
"""

# ── Standard Library ──────────────────────────────────────────────────────────
import asyncio
import csv
import json
import os
import re
import time
import traceback
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin, urlparse, quote_plus

# ── Third-party ───────────────────────────────────────────────────────────────
try:
    import aiohttp
except ImportError:
    raise SystemExit("Missing: pip install aiohttp")

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
except ImportError:
    raise SystemExit("Missing: pip install playwright && playwright install chromium")

try:
    import extruct
except ImportError:
    raise SystemExit("Missing: pip install extruct lxml cssselect")

try:
    from pydantic import BaseModel, Field, field_validator
except ImportError:
    raise SystemExit("Missing: pip install pydantic")

try:
    from tenacity import (
        retry,
        stop_after_attempt,
        wait_exponential,
        retry_if_exception_type,
    )
except ImportError:
    raise SystemExit("Missing: pip install tenacity")

try:
    from loguru import logger
except ImportError:
    raise SystemExit("Missing: pip install loguru")

try:
    from dateutil import parser as dateparser
except ImportError:
    raise SystemExit("Missing: pip install python-dateutil")

try:
    from bs4 import BeautifulSoup
except ImportError:
    raise SystemExit("Missing: pip install beautifulsoup4")

try:
    import pandas as pd
except ImportError:
    raise SystemExit("Missing: pip install pandas")

try:
    from tqdm.asyncio import tqdm as atqdm
except ImportError:
    raise SystemExit("Missing: pip install tqdm")

# ── Optional: curl_cffi for Cloudflare bypass ─────────────────────────────────
try:
    from curl_cffi.requests import AsyncSession as CurlSession
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    logger.warning("curl_cffi not available; Cloudflare bypass limited to Playwright.")

# ── Optional: LLM via litellm ─────────────────────────────────────────────────
try:
    import litellm
    HAS_LLM = True
except ImportError:
    HAS_LLM = False
    logger.warning("litellm not installed; LLM enrichment will be skipped.")

# ── Optional: instructor for structured outputs ───────────────────────────────
try:
    import instructor
    HAS_INSTRUCTOR = True
except ImportError:
    HAS_INSTRUCTOR = False

# ── Fake User-Agent ───────────────────────────────────────────────────────────
try:
    from fake_useragent import UserAgent
    UA = UserAgent()
    def random_ua():
        return UA.random
except ImportError:
    def random_ua():
        return (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )

# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═════════════════════════════════════════════════════════════════════════════

DATE_START = "2025-01-01"
DATE_END   = "2026-04-10"

CITIES = [
    # India
    "Mumbai", "Delhi", "Chennai", "Bengaluru", "Hyderabad",
    "Ahmedabad", "Kolkata", "Pune", "Jaipur", "Chandigarh",
    # USA
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
    "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
    # Europe
    "Istanbul", "Moscow", "London", "Berlin", "Madrid",
    "Kyiv", "Rome", "Paris", "Bucharest", "Vienna", "Barcelona",
    # Singapore
    "Singapore",
]

MACRO_REGION_MAP = {
    "Mumbai": "India", "Delhi": "India", "Chennai": "India", "Bengaluru": "India",
    "Hyderabad": "India", "Ahmedabad": "India", "Kolkata": "India", "Pune": "India",
    "Jaipur": "India", "Chandigarh": "India",
    "New York": "USA", "Los Angeles": "USA", "Chicago": "USA", "Houston": "USA",
    "Phoenix": "USA", "Philadelphia": "USA", "San Antonio": "USA", "San Diego": "USA",
    "Dallas": "USA", "San Jose": "USA",
    "Istanbul": "Europe", "Moscow": "Europe", "London": "Europe", "Berlin": "Europe",
    "Madrid": "Europe", "Kyiv": "Europe", "Rome": "Europe", "Paris": "Europe",
    "Bucharest": "Europe", "Vienna": "Europe", "Barcelona": "Europe",
    "Singapore": "Singapore",
}

ARTISTS = [
    "Green Day", "Shawn Mendes", "Hanumankind", "John Summit", "Aurora",
    "Glass Animals", "Louis Tomlinson", "Nothing But Thieves", "Jonita Gandhi",
    "Anushka", "Prateek Kuhad", "Seedhe Maut", "Chaar Diwaari", "Usha Uthup",
    "Euphoria", "Nucleya", "Raftaar", "Kr$na", "Sabrina Carpenter", "Justin Bieber",
    "Karol G", "Lady Gaga", "Kendrick Lamar", "The Strokes", "FKA twigs",
    "Central Cee", "Moby", "David Byrne", "Luke Combs", "Zach Bryan", "Jelly Roll",
    "T-Pain", "Nelly", "Tyler, The Creator", "Olivia Rodrigo", "Noah Kahan",
    "Charli XCX", "Neil Young", "The 1975", "Rod Stewart", "Nile Rodgers",
    "The Prodigy", "Wolf Alice", "Snow Patrol", "Turnstile", "Dua Lipa",
    "Fred again..", "Calvin Harris", "Martin Garrix", "Armin van Buuren", "Anyma",
    "Peggy Gou", "Elton John", "Foo Fighters", "The Smashing Pumpkins",
    "Alan Walker", "G-Dragon", "CL", "Babymetal", "Crowded House", "Seventeen",
    "Fatboy Slim", "Black Eyed Peas", "OneRepublic", "Jackson Wang", "Keshi",
    "JVKE", "NIKI", "Honne", "Russ", "Joji", "Bruno Major", "David Guetta",
    "Axwell",
]

# Allowed macro regions for Bandsintown filter
ALLOWED_REGIONS = {"India", "Europe", "USA", "Singapore"}

# File paths
RAW_CHECKPOINT      = Path("events_raw.json")
ENRICHED_CHECKPOINT = Path("events_enriched.json")
FINAL_CSV           = Path("events_master_database.csv")

# Concurrency limits
SCRAPE_CONCURRENCY  = 5   # Playwright workers
API_CONCURRENCY     = 10  # aiohttp workers
LLM_CONCURRENCY     = 3   # LLM call workers

# ═════════════════════════════════════════════════════════════════════════════
# PYDANTIC SCHEMAS
# ═════════════════════════════════════════════════════════════════════════════

class TicketPrice(BaseModel):
    amount: Optional[float] = None
    currency: Optional[str] = None
    tier: Optional[str] = None       # e.g. "General Admission", "VIP"

class LLMEventEnrichment(BaseModel):
    """Strict schema for LLM extraction step."""
    event_label: str = Field(
        default="Unknown",
        description=(
            "Single-word or short category: Pop, Rock, EDM, Jazz, Classical, "
            "Hip-Hop, Metal, Folk, College Fest, Tech Conference, Art Exhibition, "
            "Festival, Comedy, Sports, Theatre, Other"
        ),
    )
    sponsors: list[str] = Field(
        default_factory=list,
        description="All sponsor/partner brand names found anywhere in the text",
    )
    exhibitors: list[str] = Field(
        default_factory=list,
        description="Companies exhibiting or participating",
    )
    speakers: list[str] = Field(
        default_factory=list,
        description="Individual speakers or headlining artists",
    )
    ticket_prices: list[TicketPrice] = Field(
        default_factory=list,
        description="All ticket price tiers found",
    )
    estimated_attendance: Optional[int] = Field(
        default=None,
        description="Numeric attendance figure if found",
    )
    attendance_confidence: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="0.0=no data, 0.5=inferred, 1.0=explicitly stated",
    )
    artist_genres: list[str] = Field(
        default_factory=list,
        description="Musical genres associated with the artist/event",
    )

    @field_validator("event_label", mode="before")
    @classmethod
    def clean_label(cls, v):
        return str(v).strip().title() if v else "Unknown"


class RawEvent(BaseModel):
    """Normalised schema for a single event from any source."""
    event_id: str = ""               # SHA1 hash of name+date+venue
    event_name: str = ""
    date_raw: str = ""
    date_iso: str = ""
    venue: str = ""
    city: str = ""
    location_macro: str = ""
    ticket_links: list[str] = Field(default_factory=list)
    official_website: str = ""
    artist_genres: list[str] = Field(default_factory=list)
    source_api: str = ""
    # Enrichment fields (populated in Phase 2)
    event_label: str = ""
    sponsors: list[str] = Field(default_factory=list)
    exhibitors: list[str] = Field(default_factory=list)
    speakers: list[str] = Field(default_factory=list)
    ticket_pricing: list[dict] = Field(default_factory=list)
    estimated_attendance: Optional[int] = None
    attendance_confidence: float = 0.0

# ═════════════════════════════════════════════════════════════════════════════
# UTILITIES
# ═════════════════════════════════════════════════════════════════════════════

def make_event_id(name: str, date: str, venue: str) -> str:
    """Deterministic ID so duplicates can be detected."""
    raw = f"{name.lower().strip()}|{date.strip()}|{venue.lower().strip()}"
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def parse_date_safe(raw: str) -> str:
    """Parse any date string into ISO 8601, return '' on failure."""
    if not raw:
        return ""
    try:
        return dateparser.parse(raw, fuzzy=True).date().isoformat()
    except Exception:
        return ""


def infer_macro_region(city: str) -> str:
    """Best-effort region lookup, fallback to 'Other'."""
    for key, region in MACRO_REGION_MAP.items():
        if key.lower() in city.lower():
            return region
    return "Other"


def save_checkpoint(data: list[dict], path: Path) -> None:
    """Atomically save JSON checkpoint."""
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)
    logger.info(f"Checkpoint saved → {path} ({len(data)} records)")


def load_checkpoint(path: Path) -> list[dict]:
    """Load JSON checkpoint if it exists."""
    if path.exists():
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded checkpoint {path} ({len(data)} records)")
        return data
    return []


def deduplicate(events: list[dict]) -> list[dict]:
    """Remove duplicates by event_id (or name+date+venue hash)."""
    seen = {}
    for ev in events:
        eid = ev.get("event_id") or make_event_id(
            ev.get("event_name", ""), ev.get("date_iso", ""), ev.get("venue", "")
        )
        ev["event_id"] = eid
        seen[eid] = ev   # last-write-wins (usually fine for merging sources)
    return list(seen.values())


# ═════════════════════════════════════════════════════════════════════════════
# PHASE 1-A: ConcertArchives Scraping (Playwright + curl_cffi)
# ═════════════════════════════════════════════════════════════════════════════

CONCERTARCHIVES_BASE = "https://www.concertarchives.org/concert-search-engine"

async def scrape_concertarchives_city(
    playwright_browser,
    city: str,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """
    Scrape ConcertArchives advanced search for one city.
    Uses a Playwright browser context with stealth headers.
    """
    results = []
    async with semaphore:
        context = await playwright_browser.new_context(
            user_agent=random_ua(),
            locale="en-US",
            viewport={"width": 1280, "height": 800},
            java_script_enabled=True,
        )
        page = await context.new_page()

        # Inject minimal stealth — hide webdriver flag
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        try:
            url = (
                f"{CONCERTARCHIVES_BASE}"
                f"?search-type=advanced"
                f"&location={quote_plus(city)}"
                f"&date_from={DATE_START}"
                f"&date_to={DATE_END}"
            )
            logger.debug(f"[ConcertArchives] City={city} → {url}")
            await page.goto(url, wait_until="networkidle", timeout=30_000)
            await asyncio.sleep(2)   # polite delay

            page_num = 1
            while True:
                try:
                    # Grab all event rows on the current page
                    rows = await page.query_selector_all("table.concerts-table tbody tr")
                    if not rows:
                        # Try card layout
                        rows = await page.query_selector_all(".concert-item, .event-item")

                    for row in rows:
                        try:
                            name  = await _safe_inner_text(row, ".concert-title, td.title, .event-name")
                            date  = await _safe_inner_text(row, ".concert-date, td.date")
                            venue = await _safe_inner_text(row, ".concert-venue, td.venue")
                            link_el = await row.query_selector("a[href]")
                            link = await link_el.get_attribute("href") if link_el else ""
                            if link and not link.startswith("http"):
                                link = "https://www.concertarchives.org" + link

                            if not name:
                                continue

                            date_iso = parse_date_safe(date)
                            ev = RawEvent(
                                event_name    = name.strip(),
                                date_raw      = date.strip(),
                                date_iso      = date_iso,
                                venue         = venue.strip(),
                                city          = city,
                                location_macro= infer_macro_region(city),
                                ticket_links  = [link] if link else [],
                                official_website = link,
                                source_api    = "ConcertArchives",
                            )
                            ev.event_id = make_event_id(ev.event_name, ev.date_iso, ev.venue)
                            results.append(ev.model_dump())
                        except Exception as row_err:
                            logger.warning(f"[ConcertArchives] Row parse error ({city}): {row_err}")

                    # Pagination: look for "Next" button
                    next_btn = await page.query_selector("a[rel='next'], .pagination .next a, li.next a")
                    if not next_btn:
                        break
                    page_num += 1
                    if page_num > 20:   # hard cap
                        break
                    await next_btn.click()
                    await page.wait_for_load_state("networkidle", timeout=20_000)
                    await asyncio.sleep(1.5)

                except PWTimeout:
                    logger.warning(f"[ConcertArchives] Timeout on page {page_num} for {city}")
                    break
                except Exception as page_err:
                    logger.warning(f"[ConcertArchives] Page error ({city}, p{page_num}): {page_err}")
                    break

        except Exception as nav_err:
            logger.error(f"[ConcertArchives] Navigation error ({city}): {nav_err}")
        finally:
            await context.close()

    logger.info(f"[ConcertArchives] {city}: {len(results)} events")
    return results


async def _safe_inner_text(element, selectors: str) -> str:
    """Try multiple CSS selectors, return first non-empty text or ''."""
    for sel in selectors.split(", "):
        try:
            el = await element.query_selector(sel.strip())
            if el:
                return (await el.inner_text()).strip()
        except Exception:
            pass
    return ""


async def phase1_concertarchives() -> list[dict]:
    """Run ConcertArchives scraping for all cities concurrently."""
    logger.info("═══ Phase 1-A: ConcertArchives ═══")
    all_events: list[dict] = []
    semaphore = asyncio.Semaphore(SCRAPE_CONCURRENCY)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
            ],
        )
        tasks = [
            scrape_concertarchives_city(browser, city, semaphore)
            for city in CITIES
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await browser.close()

    for city, res in zip(CITIES, results):
        if isinstance(res, Exception):
            logger.error(f"[ConcertArchives] {city} failed: {res}")
        else:
            all_events.extend(res)

    logger.info(f"[ConcertArchives] Total events scraped: {len(all_events)}")
    return all_events


# ═════════════════════════════════════════════════════════════════════════════
# PHASE 1-B: Bandsintown API
# ═════════════════════════════════════════════════════════════════════════════

BIT_BASE = "https://rest.bandsintown.com/artists/{artist}/events"
BIT_APP_ID = os.getenv("BANDSINTOWN_APP_ID", "test")   # replace with real app_id

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
)
async def fetch_bandsintown_artist(
    session: aiohttp.ClientSession,
    artist: str,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Fetch events for one artist from Bandsintown, filtered to allowed regions."""
    async with semaphore:
        url = BIT_BASE.format(artist=quote_plus(artist))
        params = {
            "app_id": BIT_APP_ID,
            "date": f"{DATE_START},{DATE_END}",
        }
        results = []
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 10))
                    logger.warning(f"[Bandsintown] Rate-limited for {artist}, sleeping {retry_after}s")
                    await asyncio.sleep(retry_after)
                    raise aiohttp.ClientError("Rate limited")
                if resp.status != 200:
                    logger.warning(f"[Bandsintown] {artist} → HTTP {resp.status}")
                    return []
                data = await resp.json(content_type=None)

                if not isinstance(data, list):
                    return []

                for ev in data:
                    try:
                        venue_info = ev.get("venue", {})
                        city    = venue_info.get("city", "")
                        country = venue_info.get("country", "")
                        region  = infer_macro_region(city) if city else "Other"

                        # Country-based fallback for region classification
                        if region == "Other":
                            COUNTRY_MAP = {
                                "IN": "India", "India": "India",
                                "SG": "Singapore", "Singapore": "Singapore",
                                "US": "USA", "United States": "USA",
                            }
                            for k in list(COUNTRY_MAP.keys()):
                                if k.lower() in country.lower():
                                    region = COUNTRY_MAP[k]
                                    break
                            # European countries fallback
                            EU_COUNTRIES = {
                                "UK", "United Kingdom", "Germany", "France",
                                "Spain", "Italy", "Turkey", "Romania",
                                "Austria", "Russia", "Ukraine",
                            }
                            for ec in EU_COUNTRIES:
                                if ec.lower() in country.lower():
                                    region = "Europe"
                                    break

                        if region not in ALLOWED_REGIONS:
                            continue

                        offers = ev.get("offers", [])
                        ticket_links = [o.get("url", "") for o in offers if o.get("url")]

                        raw_ev = RawEvent(
                            event_name     = ev.get("title") or f"{artist} Live",
                            date_raw       = ev.get("datetime", ""),
                            date_iso       = parse_date_safe(ev.get("datetime", "")),
                            venue          = venue_info.get("name", ""),
                            city           = city,
                            location_macro = region,
                            ticket_links   = ticket_links,
                            official_website = ev.get("url", ""),
                            artist_genres  = [],          # enriched later
                            source_api     = "Bandsintown",
                        )
                        raw_ev.event_id = make_event_id(raw_ev.event_name, raw_ev.date_iso, raw_ev.venue)
                        results.append(raw_ev.model_dump())
                    except Exception as parse_err:
                        logger.warning(f"[Bandsintown] Parse error ({artist}): {parse_err}")

        except Exception as e:
            logger.error(f"[Bandsintown] Error fetching {artist}: {e}")
            return []

        return results


async def phase1_bandsintown() -> list[dict]:
    """Fetch all artist events from Bandsintown concurrently."""
    logger.info("═══ Phase 1-B: Bandsintown ═══")
    semaphore = asyncio.Semaphore(API_CONCURRENCY)
    headers = {"User-Agent": random_ua(), "Accept": "application/json"}
    all_events: list[dict] = []

    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [
            fetch_bandsintown_artist(session, artist, semaphore)
            for artist in ARTISTS
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for artist, res in zip(ARTISTS, results):
        if isinstance(res, Exception):
            logger.error(f"[Bandsintown] {artist} failed: {res}")
        else:
            all_events.extend(res)

    logger.info(f"[Bandsintown] Total events: {len(all_events)}")
    return all_events


# ═════════════════════════════════════════════════════════════════════════════
# PHASE 1-C: Eventbrite API
# ═════════════════════════════════════════════════════════════════════════════

EB_BASE  = "https://www.eventbriteapi.com/v3"
EB_TOKEN = os.getenv("EVENTBRITE_TOKEN", "")

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=3, max=60),
    retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
)
async def fetch_eventbrite_city(
    session: aiohttp.ClientSession,
    city: str,
    semaphore: asyncio.Semaphore,
) -> list[dict]:
    """Search Eventbrite events for one city with pagination."""
    if not EB_TOKEN:
        return []   # skip if no token configured

    async with semaphore:
        results = []
        page = 1
        while True:
            params = {
                "q": city,
                "location.address": city,
                "location.within": "50km",
                "start_date.range_start": f"{DATE_START}T00:00:00Z",
                "start_date.range_end": f"{DATE_END}T23:59:59Z",
                "page": page,
                "expand": "venue,ticket_classes",
                "token": EB_TOKEN,
            }
            headers = {
                "Authorization": f"Bearer {EB_TOKEN}",
                "Accept": "application/json",
            }
            try:
                async with session.get(
                    f"{EB_BASE}/events/search",
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(30)
                        raise aiohttp.ClientError("EB rate limited")
                    if resp.status == 401:
                        logger.error("[Eventbrite] Invalid token — skipping all EB fetches")
                        return []
                    if resp.status != 200:
                        logger.warning(f"[Eventbrite] {city} p{page} → HTTP {resp.status}")
                        break
                    data = await resp.json()

                    events = data.get("events", [])
                    pagination = data.get("pagination", {})

                    for ev in events:
                        try:
                            venue_info = ev.get("venue") or {}
                            addr = venue_info.get("address", {})
                            ev_city = addr.get("city", city)
                            region = infer_macro_region(ev_city)

                            tix = ev.get("ticket_classes", [])
                            ticket_links = [ev.get("url", "")] if ev.get("url") else []
                            prices = []
                            for t in tix:
                                cost = t.get("cost", {})
                                if cost:
                                    prices.append({
                                        "amount": cost.get("major_value"),
                                        "currency": cost.get("currency"),
                                        "tier": t.get("name"),
                                    })

                            raw_ev = RawEvent(
                                event_name     = ev.get("name", {}).get("text", ""),
                                date_raw       = ev.get("start", {}).get("local", ""),
                                date_iso       = parse_date_safe(
                                    ev.get("start", {}).get("local", "")
                                ),
                                venue          = venue_info.get("name", ""),
                                city           = ev_city,
                                location_macro = region,
                                ticket_links   = ticket_links,
                                official_website = ev.get("url", ""),
                                source_api     = "Eventbrite",
                            )
                            raw_ev.ticket_pricing = prices
                            raw_ev.event_id = make_event_id(
                                raw_ev.event_name, raw_ev.date_iso, raw_ev.venue
                            )
                            results.append(raw_ev.model_dump())
                        except Exception as pe:
                            logger.warning(f"[Eventbrite] Parse error ({city}): {pe}")

                    if not pagination.get("has_more_items", False) or page >= 10:
                        break
                    page += 1
                    await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(f"[Eventbrite] {city} p{page}: {e}")
                break

        return results


async def phase1_eventbrite() -> list[dict]:
    """Fetch all city events from Eventbrite concurrently."""
    logger.info("═══ Phase 1-C: Eventbrite ═══")
    if not EB_TOKEN:
        logger.warning("[Eventbrite] No EVENTBRITE_TOKEN set — skipping.")
        return []

    semaphore = asyncio.Semaphore(API_CONCURRENCY)
    all_events: list[dict] = []

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_eventbrite_city(session, city, semaphore)
            for city in CITIES
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for city, res in zip(CITIES, results):
        if isinstance(res, Exception):
            logger.error(f"[Eventbrite] {city} failed: {res}")
        else:
            all_events.extend(res)

    logger.info(f"[Eventbrite] Total events: {len(all_events)}")
    return all_events


# ═════════════════════════════════════════════════════════════════════════════
# PHASE 1 ORCHESTRATOR
# ═════════════════════════════════════════════════════════════════════════════

async def run_phase1() -> list[dict]:
    """
    Orchestrate all three sources, combine, deduplicate, and checkpoint.
    Skips re-running if checkpoint already exists.
    """
    if RAW_CHECKPOINT.exists():
        logger.info(f"Phase 1 checkpoint found — loading {RAW_CHECKPOINT}")
        return load_checkpoint(RAW_CHECKPOINT)

    logger.info("Starting Phase 1: Data Ingestion")

    # Run all three sources; failures in one don't block others
    ca_events, bit_events, eb_events = await asyncio.gather(
        phase1_concertarchives(),
        phase1_bandsintown(),
        phase1_eventbrite(),
        return_exceptions=True,
    )

    all_events: list[dict] = []
    for label, batch in [
        ("ConcertArchives", ca_events),
        ("Bandsintown", bit_events),
        ("Eventbrite", eb_events),
    ]:
        if isinstance(batch, Exception):
            logger.error(f"Phase 1 source '{label}' raised: {batch}")
        elif isinstance(batch, list):
            all_events.extend(batch)

    all_events = deduplicate(all_events)
    save_checkpoint(all_events, RAW_CHECKPOINT)
    return all_events


# ═════════════════════════════════════════════════════════════════════════════
# PHASE 2: DEEP ENRICHMENT PIPELINE
# ═════════════════════════════════════════════════════════════════════════════

# ─── 2-A: Fetch raw HTML ─────────────────────────────────────────────────────

async def fetch_html_aiohttp(session: aiohttp.ClientSession, url: str) -> str:
    """Attempt lightweight HTML fetch with aiohttp."""
    try:
        async with session.get(
            url,
            headers={"User-Agent": random_ua()},
            timeout=aiohttp.ClientTimeout(total=15),
            allow_redirects=True,
            ssl=False,
        ) as resp:
            if resp.status == 200:
                return await resp.text(errors="replace")
    except Exception as e:
        logger.debug(f"[aiohttp] {url}: {e}")
    return ""


async def fetch_html_playwright(pw_browser, url: str) -> str:
    """JS-rendered fetch via Playwright when aiohttp returns empty/blocked."""
    try:
        ctx = await pw_browser.new_context(user_agent=random_ua())
        page = await ctx.new_page()
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=25_000)
        await asyncio.sleep(1.5)
        html = await page.content()
        await ctx.close()
        return html
    except Exception as e:
        logger.debug(f"[Playwright] {url}: {e}")
        return ""


# ─── 2-B: extruct structured data extraction ────────────────────────────────

def extract_structured_data(html: str, base_url: str) -> dict:
    """
    Use extruct to parse JSON-LD / Microdata from the page.
    Returns a flattened dict of any schema.org/Event fields found.
    """
    result = {}
    if not html:
        return result
    try:
        data = extruct.extract(
            html,
            base_url=base_url,
            uniform=True,
            syntaxes=["json-ld", "microdata", "opengraph"],
        )
        for syntax in ("json-ld", "microdata"):
            for item in data.get(syntax, []):
                item_type = item.get("@type", "")
                if isinstance(item_type, list):
                    item_type = " ".join(item_type)
                if "Event" in item_type:
                    result["name"]        = item.get("name", "")
                    result["startDate"]   = item.get("startDate", "")
                    result["endDate"]     = item.get("endDate", "")
                    result["location"]    = item.get("location", {})
                    result["offers"]      = item.get("offers", [])
                    result["organizer"]   = item.get("organizer", {})
                    result["sponsor"]     = item.get("sponsor", [])
                    result["performer"]   = item.get("performer", [])
                    result["description"] = item.get("description", "")
                    result["attendanceMode"] = item.get("eventAttendanceMode", "")
                    break
            if result:
                break
    except Exception as e:
        logger.debug(f"[extruct] parse error: {e}")
    return result


# ─── 2-C: Subpage scraping ───────────────────────────────────────────────────

SUBPAGES = ["/tickets", "/speakers", "/agenda", "/sponsors", "/partners",
            "/exhibitors", "/about", "/lineup"]

async def scrape_subpages(
    session: aiohttp.ClientSession,
    pw_browser,
    base_url: str,
) -> str:
    """
    Try to fetch common subpages and return concatenated text.
    Falls back to Playwright for JS-heavy pages.
    """
    parsed = urlparse(base_url)
    root = f"{parsed.scheme}://{parsed.netloc}"
    texts = []
    for sub in SUBPAGES:
        url = urljoin(root, sub)
        html = await fetch_html_aiohttp(session, url)
        if not html:
            html = await fetch_html_playwright(pw_browser, url)
        if html:
            soup = BeautifulSoup(html, "lxml")
            texts.append(soup.get_text(separator=" ", strip=True)[:3000])
    return " ".join(texts)


# ─── 2-D: Attendance fallback via text search ────────────────────────────────

ATTENDANCE_PATTERNS = [
    re.compile(r"(\d[\d,]+)\s*(?:attendees?|guests?|visitors?)", re.I),
    re.compile(r"capacity[:\s]+(\d[\d,]+)", re.I),
    re.compile(r"expected\s+attendance[:\s]+(\d[\d,]+)", re.I),
    re.compile(r"(\d[\d,]+)\s*people?\s+(?:expected|attended)", re.I),
    re.compile(r"sold[\s-]out\s+(?:crowd|show|venue)", re.I),
]

def extract_attendance_from_text(text: str) -> tuple[Optional[int], float]:
    """
    Scan raw text for attendance figures.
    Returns (figure_or_None, confidence_score).
    """
    for pat in ATTENDANCE_PATTERNS[:-1]:   # numeric patterns
        m = pat.search(text)
        if m:
            try:
                num = int(m.group(1).replace(",", ""))
                confidence = 0.7
                return num, confidence
            except Exception:
                pass
    # Sold-out mention without number → low confidence estimate
    if ATTENDANCE_PATTERNS[-1].search(text):
        return None, 0.4
    return None, 0.0


# ─── 2-E: LLM enrichment ─────────────────────────────────────────────────────

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "gemini")

SYSTEM_PROMPT = """You are an expert event data analyst. Extract structured metadata from event page text.
Return ONLY a valid JSON object matching this schema:
{
  "event_label": string,
  "sponsors": [string],
  "exhibitors": [string],
  "speakers": [string],
  "ticket_prices": [{"amount": number|null, "currency": string|null, "tier": string|null}],
  "estimated_attendance": number|null,
  "attendance_confidence": float (0.0–1.0),
  "artist_genres": [string]
}
No preamble, no markdown fences — raw JSON only."""

async def llm_enrich(text: str, event_name: str) -> Optional[LLMEventEnrichment]:
    """
    Call LLM to enrich event with structured metadata.
    Returns None if LLM unavailable or call fails.
    """
    if not HAS_LLM:
        return None
    if not text.strip():
        return None

    # Truncate to avoid token limits
    truncated = text[:6000]
    prompt = (
        f"Event: {event_name}\n\n"
        f"Page content:\n{truncated}\n\n"
        "Extract the structured metadata."
    )

    model_map = {
        "gemini": "gemini/gemini-2.0-flash-lite",
    }
    model = model_map.get(LLM_PROVIDER, "gemini/gemini-2.0-flash-lite")

    try:
        resp = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: litellm.completion(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
                max_tokens=800,
            ),
        )
        raw_json = resp.choices[0].message.content.strip()
        # Strip accidental markdown fences
        raw_json = re.sub(r"^```(?:json)?|```$", "", raw_json, flags=re.M).strip()
        data = json.loads(raw_json)
        return LLMEventEnrichment(**data)
    except Exception as e:
        logger.debug(f"[LLM] Enrichment failed for '{event_name}': {e}")
        return None


# ─── 2-F: Per-event enrichment ───────────────────────────────────────────────

async def enrich_event(
    event: dict,
    session: aiohttp.ClientSession,
    pw_browser,
    llm_semaphore: asyncio.Semaphore,
    scrape_semaphore: asyncio.Semaphore,
) -> dict:
    """
    Full enrichment waterfall for a single event.
    Mutates the event dict in-place and returns it.
    """
    event_name = event.get("event_name", "Unknown Event")
    url = event.get("official_website") or (
        event.get("ticket_links", [None])[0] if event.get("ticket_links") else None
    )

    if not url or not url.startswith("http"):
        return event  # nothing to enrich

    async with scrape_semaphore:
        # ── Step 1: Fast HTML fetch ──────────────────────────────────────────
        html = await fetch_html_aiohttp(session, url)

        # ── Step 2: JS fallback if page is blank/blocked ─────────────────────
        if len(html) < 500:
            html = await fetch_html_playwright(pw_browser, url)

        # ── Step 3: extruct — structured data (fast path) ────────────────────
        structured = extract_structured_data(html, url)

        # ── Step 4: Subpage scraping (slow path) if key fields missing ────────
        soup = BeautifulSoup(html, "lxml")
        page_text = soup.get_text(separator=" ", strip=True)

        need_subpages = not (structured.get("offers") or structured.get("sponsor"))
        if need_subpages:
            subpage_text = await scrape_subpages(session, pw_browser, url)
            page_text += " " + subpage_text

        # ── Step 5: Attendance fallback ───────────────────────────────────────
        att_num, att_conf = extract_attendance_from_text(page_text)
        if att_num and not event.get("estimated_attendance"):
            event["estimated_attendance"]  = att_num
            event["attendance_confidence"] = att_conf

        # ── Step 6: Parse sponsors/prices from structured data ────────────────
        if structured.get("sponsor"):
            sp = structured["sponsor"]
            if isinstance(sp, list):
                event["sponsors"] = [
                    s.get("name", str(s)) if isinstance(s, dict) else str(s)
                    for s in sp
                ]
            elif isinstance(sp, dict):
                event["sponsors"] = [sp.get("name", "")]

        if structured.get("offers") and not event.get("ticket_pricing"):
            offers = structured["offers"]
            if not isinstance(offers, list):
                offers = [offers]
            prices = []
            for o in offers:
                if isinstance(o, dict):
                    prices.append({
                        "amount": o.get("price"),
                        "currency": o.get("priceCurrency"),
                        "tier": o.get("name"),
                    })
            event["ticket_pricing"] = prices

    # ── Step 7: LLM deep extraction ───────────────────────────────────────────
    async with llm_semaphore:
        enrichment = await llm_enrich(page_text[:6000], event_name)
        if enrichment:
            if not event.get("event_label"):
                event["event_label"] = enrichment.event_label
            if not event.get("sponsors"):
                event["sponsors"] = enrichment.sponsors
            if not event.get("exhibitors"):
                event["exhibitors"] = enrichment.exhibitors
            if not event.get("speakers"):
                event["speakers"] = enrichment.speakers
            if not event.get("ticket_pricing"):
                event["ticket_pricing"] = [p.model_dump() for p in enrichment.ticket_prices]
            if not event.get("estimated_attendance") and enrichment.estimated_attendance:
                event["estimated_attendance"] = enrichment.estimated_attendance
                event["attendance_confidence"] = max(
                    event.get("attendance_confidence", 0.0),
                    enrichment.attendance_confidence,
                )
            if enrichment.artist_genres and not event.get("artist_genres"):
                event["artist_genres"] = enrichment.artist_genres

    return event


async def run_phase2(raw_events: list[dict]) -> list[dict]:
    """
    Enrich all events asynchronously.
    Saves checkpoint every 100 events to prevent data loss.
    """
    logger.info(f"═══ Phase 2: Enrichment ({len(raw_events)} events) ═══")

    # Load existing enriched checkpoint to resume
    enriched_map: dict[str, dict] = {}
    existing = load_checkpoint(ENRICHED_CHECKPOINT)
    for ev in existing:
        enriched_map[ev["event_id"]] = ev

    to_enrich = [ev for ev in raw_events if ev["event_id"] not in enriched_map]
    logger.info(f"Resuming enrichment: {len(to_enrich)} remaining / {len(raw_events)} total")

    llm_sem   = asyncio.Semaphore(LLM_CONCURRENCY)
    scrape_sem = asyncio.Semaphore(SCRAPE_CONCURRENCY)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        async with aiohttp.ClientSession() as session:
            batch_size = 50
            for i in range(0, len(to_enrich), batch_size):
                batch = to_enrich[i : i + batch_size]
                tasks = [
                    enrich_event(ev, session, browser, llm_sem, scrape_sem)
                    for ev in batch
                ]
                enriched_batch = await asyncio.gather(*tasks, return_exceptions=True)
                for orig, result in zip(batch, enriched_batch):
                    if isinstance(result, Exception):
                        logger.warning(f"[Enrich] {orig.get('event_name')}: {result}")
                        enriched_map[orig["event_id"]] = orig
                    else:
                        enriched_map[result["event_id"]] = result

                # Checkpoint every batch
                save_checkpoint(list(enriched_map.values()), ENRICHED_CHECKPOINT)
                logger.info(
                    f"Enrichment progress: {min(i + batch_size, len(to_enrich))}"
                    f"/{len(to_enrich)}"
                )

        await browser.close()

    return list(enriched_map.values())


# ═════════════════════════════════════════════════════════════════════════════
# PHASE 3: OUTPUT FORMATTING
# ═════════════════════════════════════════════════════════════════════════════

CSV_COLUMNS = [
    "Event_Name", "Event_Label", "Location_Macro", "City", "Venue", "Date",
    "Sponsors", "Participating_Exhibitors", "Speakers", "Ticket_Pricing",
    "Estimated_Attendance", "Attendance_Confidence", "Artist_Genres",
    "Ticket_Links", "Official_Websites", "Source_API",
]

def format_list(lst: Any) -> str:
    """Flatten a list to a pipe-separated string."""
    if not lst:
        return ""
    if isinstance(lst, str):
        return lst
    if isinstance(lst, list):
        parts = []
        for item in lst:
            if isinstance(item, dict):
                parts.append(
                    " | ".join(f"{k}:{v}" for k, v in item.items() if v is not None)
                )
            else:
                parts.append(str(item))
        return " || ".join(parts)
    return str(lst)


def run_phase3(enriched_events: list[dict]) -> None:
    """Write the final beautified CSV."""
    logger.info(f"═══ Phase 3: Export ({len(enriched_events)} events) ═══")

    rows = []
    for ev in enriched_events:
        rows.append({
            "Event_Name":             ev.get("event_name", ""),
            "Event_Label":            ev.get("event_label", ""),
            "Location_Macro":         ev.get("location_macro", ""),
            "City":                   ev.get("city", ""),
            "Venue":                  ev.get("venue", ""),
            "Date":                   ev.get("date_iso", ev.get("date_raw", "")),
            "Sponsors":               format_list(ev.get("sponsors", [])),
            "Participating_Exhibitors": format_list(ev.get("exhibitors", [])),
            "Speakers":               format_list(ev.get("speakers", [])),
            "Ticket_Pricing":         format_list(ev.get("ticket_pricing", [])),
            "Estimated_Attendance":   ev.get("estimated_attendance", ""),
            "Attendance_Confidence":  ev.get("attendance_confidence", ""),
            "Artist_Genres":          format_list(ev.get("artist_genres", [])),
            "Ticket_Links":           format_list(ev.get("ticket_links", [])),
            "Official_Websites":      ev.get("official_website", ""),
            "Source_API":             ev.get("source_api", ""),
        })

    df = pd.DataFrame(rows, columns=CSV_COLUMNS)
    df.sort_values(["Date", "Location_Macro", "City"], inplace=True)
    df.to_csv(FINAL_CSV, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_ALL)
    logger.success(f"✅ Final CSV written → {FINAL_CSV} ({len(df)} rows)")


# ═════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

async def main():
    logger.info("🎵  Event Pipeline Starting  🎵")
    logger.info(f"Date range: {DATE_START} → {DATE_END}")
    logger.info(f"Cities: {len(CITIES)} | Artists: {len(ARTISTS)}")

    # ── Phase 1 ────────────────────────────────────────────────────────────────
    try:
        raw_events = await run_phase1()
        logger.info(f"Phase 1 complete: {len(raw_events)} unique events")
    except Exception as e:
        logger.critical(f"Phase 1 catastrophic failure: {e}\n{traceback.format_exc()}")
        raw_events = load_checkpoint(RAW_CHECKPOINT)
        if not raw_events:
            logger.error("No data to proceed with. Exiting.")
            return

    # ── Phase 2 ────────────────────────────────────────────────────────────────
    try:
        enriched_events = await run_phase2(raw_events)
        logger.info(f"Phase 2 complete: {len(enriched_events)} enriched events")
    except Exception as e:
        logger.critical(f"Phase 2 catastrophic failure: {e}\n{traceback.format_exc()}")
        enriched_events = load_checkpoint(ENRICHED_CHECKPOINT) or raw_events
        logger.warning(f"Falling back to {len(enriched_events)} events for Phase 3")

    # ── Phase 3 ────────────────────────────────────────────────────────────────
    try:
        run_phase3(enriched_events)
    except Exception as e:
        logger.critical(f"Phase 3 failed: {e}\n{traceback.format_exc()}")

    logger.info("🏁  Pipeline finished.")


if __name__ == "__main__":
    # ── Logging configuration ──────────────────────────────────────────────────
    logger.remove()
    logger.add(
        "pipeline.log",
        rotation="50 MB",
        retention="7 days",
        level="DEBUG",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
    )
    logger.add(
        lambda msg: print(msg, end=""),
        level="INFO",
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    )

    asyncio.run(main())