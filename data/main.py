"""
=============================================================================
  Bandsintown Event Data Extractor, Filter & LLM Enricher
=============================================================================

DEPENDENCIES — install once before running:
    pip install requests google-generativeai

API KEYS — set your credentials in the two constants below:
    BANDSINTOWN_APP_ID  : Any non-empty string works for the public API
                          (Bandsintown asks you to use your own app name as
                           the app_id, e.g. "MyApp").
    GEMINI_API_KEY      : Your Google Gemini API key.
                          Get one free at https://aistudio.google.com/app/apikey

OUTPUT:
    enriched_tour_data.json  — written to the same directory as this script.
=============================================================================
"""

import json
import time
import urllib.parse
import requests
from dotenv import load_dotenv
import os
load_dotenv()

# ---------------------------------------------------------------------------
# 0.  CONFIGURATION  — insert your keys here
# ---------------------------------------------------------------------------

BANDSINTOWN_APP_ID = os.getenv("BANDSINTOWN_APP_ID")      # ← replace
GEMINI_API_KEY     = os.getenv("GEMINI_API_KEY")   # ← replace  (aistudio.google.com)

# Bandsintown base URLs (from official docs)
BIT_BASE        = "https://rest.bandsintown.com"
EVENTS_ENDPOINT = "{base}/artists/{name}/events/?app_id={app_id}&date=past"
ARTIST_ENDPOINT = "{base}/artists/{name}/?app_id={app_id}"

OUTPUT_FILE = "enriched_tour_data.json"

# How long to wait between successive Bandsintown artist requests (seconds)
REQUEST_DELAY = 0.75

# ---------------------------------------------------------------------------
# 1.  ARTIST LIST
# ---------------------------------------------------------------------------

ARTISTS = [
    "Green Day", "Shawn Mendes", "Hanumankind", "John Summit", "Aurora",
    "Glass Animals", "Louis Tomlinson", "Nothing But Thieves", "Jonita Gandhi",
    "Anushka", "Prateek Kuhad", "Seedhe Maut", "Chaar Diwaari", "Usha Uthup",
    "Euphoria", "Nucleya", "Raftaar", "Kr$na", "Sabrina Carpenter",
    "Justin Bieber", "Karol G", "Lady Gaga", "Kendrick Lamar", "The Strokes",
    "FKA twigs", "Central Cee", "Moby", "David Byrne", "Luke Combs",
    "Zach Bryan", "Jelly Roll", "T-Pain", "Nelly", "Tyler, The Creator",
    "Olivia Rodrigo", "Noah Kahan", "Charli XCX", "Neil Young", "The 1975",
    "Rod Stewart", "Nile Rodgers", "The Prodigy", "Wolf Alice", "Snow Patrol",
    "Turnstile", "Dua Lipa", "Fred again..", "Calvin Harris", "Martin Garrix",
    "Armin van Buuren", "Anyma", "Peggy Gou", "Elton John", "Foo Fighters",
    "The Smashing Pumpkins", "Alan Walker", "G-Dragon", "CL", "Babymetal",
    "Crowded House", "Seventeen", "Fatboy Slim", "Black Eyed Peas",
    "OneRepublic", "Jackson Wang", "Keshi", "JVKE", "NIKI", "Honne", "Russ",
    "Joji", "Bruno Major", "David Guetta", "Axwell",
]

# ---------------------------------------------------------------------------
# 2.  LOCATION FILTER
# ---------------------------------------------------------------------------

# Comprehensive list of sovereign European countries (UN / Council of Europe)
EUROPEAN_COUNTRIES = {
    "Albania", "Andorra", "Armenia", "Austria", "Azerbaijan", "Belarus",
    "Belgium", "Bosnia and Herzegovina", "Bulgaria", "Croatia", "Cyprus",
    "Czech Republic", "Czechia", "Denmark", "Estonia", "Finland", "France",
    "Georgia", "Germany", "Greece", "Hungary", "Iceland", "Ireland", "Italy",
    "Kazakhstan", "Kosovo", "Latvia", "Liechtenstein", "Lithuania",
    "Luxembourg", "Malta", "Moldova", "Monaco", "Montenegro",
    "Netherlands", "North Macedonia", "Norway", "Poland", "Portugal",
    "Romania", "Russia", "San Marino", "Serbia", "Slovakia", "Slovenia",
    "Spain", "Sweden", "Switzerland", "Turkey", "Ukraine",
    "United Kingdom", "UK", "England", "Scotland", "Wales",
    "Vatican City", "Holy See",
}

USA_VARIANTS = {"united states", "us", "usa", "u.s.", "u.s.a."}

def is_target_location(venue: dict) -> bool:
    """Return True if the venue is in India, USA, Singapore, or Europe."""
    country = (venue.get("country") or "").strip()
    country_lower = country.lower()

    if country_lower == "india":
        return True
    if country_lower in USA_VARIANTS:
        return True
    if country_lower == "singapore":
        return True
    if country in EUROPEAN_COUNTRIES:
        return True
    # Also handle case-insensitive European match
    if country.title() in EUROPEAN_COUNTRIES or country.upper() in EUROPEAN_COUNTRIES:
        return True
    return False

# ---------------------------------------------------------------------------
# 3.  BANDSINTOWN API HELPERS
# ---------------------------------------------------------------------------

HEADERS = {"Accept": "application/json", "User-Agent": "TourDataEnricher/1.0"}

def fetch_artist_info(artist_name: str) -> dict:
    """Fetch top-level artist metadata (image, URL, etc.) from Bandsintown."""
    encoded = urllib.parse.quote(artist_name)
    url = ARTIST_ENDPOINT.format(
        base=BIT_BASE, name=encoded, app_id=BANDSINTOWN_APP_ID
    )
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json()


def fetch_events(artist_name: str) -> list:
    """Fetch upcoming events for a single artist. Returns list of event dicts."""
    encoded = urllib.parse.quote(artist_name)
    url = EVENTS_ENDPOINT.format(
        base=BIT_BASE, name=encoded, app_id=BANDSINTOWN_APP_ID
    )
    resp = requests.get(url, headers=HEADERS, timeout=15)

    # 404 → artist not found on Bandsintown
    if resp.status_code == 404:
        print(f"    [404] Artist not found on Bandsintown: {artist_name}")
        return []

    resp.raise_for_status()

    data = resp.json()

    # Guard: Bandsintown sometimes returns {"errors": [...]} or a warning string
    if isinstance(data, dict):
        print(f"    [WARN] Unexpected dict response for '{artist_name}': {data}")
        return []

    return data  # list of event objects

# ---------------------------------------------------------------------------
# 4.  EVENT FIELD EXTRACTION
# ---------------------------------------------------------------------------

def extract_ticket_links(offers: list) -> list[dict]:
    """Parse the offers array into a clean list of ticket / stream links."""
    links = []
    for offer in (offers or []):
        links.append({
            "type":   offer.get("type", ""),
            "status": offer.get("status", ""),
            "url":    offer.get("url", ""),
        })
    return links


def parse_event(event: dict, artist_name: str, artist_info: dict) -> dict:
    """
    Map a raw Bandsintown event dict → our enriched event dict.

    Bandsintown event schema (from SwaggerHub 3.0.1):
      id, artist_id, url, on_sale_datetime, datetime, description,
      title, lineup[], offers[], venue{}, artist{}
    """
    venue   = event.get("venue") or {}
    offers  = event.get("offers") or []
    lineup  = event.get("lineup") or []

    # Artist URL from artist_info (top-level artist fetch)
    artist_url = artist_info.get("url", "") if isinstance(artist_info, dict) else ""

    # Some events carry their own embedded artist object
    embedded_artist = event.get("artist") or {}

    return {
        # ── Identity ────────────────────────────────────────────────────────
        "event_id":          event.get("id", ""),
        "title":             event.get("title") or f"{artist_name} Live",
        "description":       event.get("description", ""),

        # ── Date / Time ─────────────────────────────────────────────────────
        "datetime":          event.get("datetime", ""),
        "on_sale_datetime":  event.get("on_sale_datetime", ""),

        # ── Venue & Location ────────────────────────────────────────────────
        "venue": {
            "name":    venue.get("name", ""),
            "city":    venue.get("city", ""),
            "region":  venue.get("region", ""),
            "country": venue.get("country", ""),
            "latitude":  venue.get("latitude", ""),
            "longitude": venue.get("longitude", ""),
        },

        # ── Artist ──────────────────────────────────────────────────────────
        "artist":      artist_name,
        "lineup":      lineup,

        # ── Tickets ─────────────────────────────────────────────────────────
        "ticket_links": extract_ticket_links(offers),

        # ── URLs ────────────────────────────────────────────────────────────
        "event_url":   event.get("url", ""),
        "artist_url":  artist_url,
        "artist_image_url": (
            artist_info.get("image_url", "")
            if isinstance(artist_info, dict) else ""
        ),
        "artist_thumb_url": (
            embedded_artist.get("thumb_url", "")
            or (artist_info.get("thumb_url", "") if isinstance(artist_info, dict) else "")
        ),

        # ── Extras ──────────────────────────────────────────────────────────
        "bandsintown_artist_id": event.get("artist_id", ""),
        # Sponsors are not part of the public API v3 schema; placeholder kept
        # so the field is present for manual enrichment.
        "sponsors": [],
    }

# ---------------------------------------------------------------------------
# 6.  MAIN PIPELINE
# ---------------------------------------------------------------------------

def main():
    print("=" * 65)
    print("  Bandsintown Event Enricher — starting run")
    print("=" * 65)

    all_enriched_events: list[dict] = []
    skipped_artists:     list[str]  = []
    total_raw           = 0
    total_kept          = 0

    for idx, artist_name in enumerate(ARTISTS, start=1):
        print(f"\n[{idx:02d}/{len(ARTISTS)}] Processing: {artist_name}")

        # ── 6a.  Fetch artist metadata ────────────────────────────────────
        artist_info = {}
        try:
            artist_info = fetch_artist_info(artist_name)
            if isinstance(artist_info, dict) and artist_info.get("name"):
                # Use the official name returned by the API for subsequent fetching
                artist_name = artist_info["name"]
        except Exception as exc:
            print(f"    [WARN] Could not fetch artist info: {exc}")

        # ── 6b.  Fetch events ─────────────────────────────────────────────
        try:
            events = fetch_events(artist_name)
        except requests.exceptions.HTTPError as http_err:
            print(f"    [HTTP ERROR] {http_err}")
            skipped_artists.append(artist_name)
            time.sleep(REQUEST_DELAY)
            continue
        except requests.exceptions.RequestException as req_err:
            print(f"    [REQUEST ERROR] {req_err}")
            skipped_artists.append(artist_name)
            time.sleep(REQUEST_DELAY)
            continue
        except Exception as exc:
            print(f"    [UNEXPECTED ERROR] {exc}")
            skipped_artists.append(artist_name)
            time.sleep(REQUEST_DELAY)
            continue

        print(f"    Raw events fetched : {len(events)}")
        total_raw += len(events)

        # ── 6c.  Filter + parse events ────────────────────────────────────
        kept = 0
        for event in events:
            venue = event.get("venue") or {}
            if not is_target_location(venue):
                continue
            enriched = parse_event(event, artist_name, artist_info)
            all_enriched_events.append(enriched)
            kept += 1

        total_kept += kept
        print(f"    Kept after filter  : {kept}")

        time.sleep(REQUEST_DELAY)

    # ── 7.  Write output ──────────────────────────────────────────────────
    with open(OUTPUT_FILE, "w", encoding="utf-8") as fh:
        json.dump(all_enriched_events, fh, indent=2, ensure_ascii=False)

    print("\n" + "=" * 65)
    print(f"  Run complete")
    print(f"  Artists processed  : {len(ARTISTS) - len(skipped_artists)} / {len(ARTISTS)}")
    print(f"  Artists skipped    : {len(skipped_artists)}  {skipped_artists}")
    print(f"  Total raw events   : {total_raw}")
    print(f"  Events kept        : {total_kept}")
    print(f"  Output file        : {OUTPUT_FILE}")
    print("=" * 65)


if __name__ == "__main__":
    main()