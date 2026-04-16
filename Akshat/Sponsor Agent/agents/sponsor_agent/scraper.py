"""
Sponsor Data Scraper -- collects real sponsor data from public conference websites.

Strategy:
  1. Scrape conf.tech for tech conference listings in the target category/geography
  2. Visit curated conference sponsor pages and extract sponsor names/tiers
  3. Deduplicate and store results as JSON for the ranking engine

Sources targeted:
  - conf.tech (tech conference listings)
  - Direct conference sponsor pages (NeurIPS, ICML, ICLR, PyCon, EthGlobal, etc.)
"""

import json
import os
import re
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from config import (
    MAX_RETRIES,
    REQUEST_DELAY,
    REQUEST_TIMEOUT,
    SCRAPED_DIR,
    USER_AGENT,
)
from agents.sponsor_agent.schemas import PastSponsorship, Sponsor


class SponsorScraper:
    """Scrapes sponsor data from public conference/event websites."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._scraped_sponsors: List[Dict[str, Any]] = []

    # ── Public API ──────────────────────────────────────────────────────

    def scrape_all(
        self,
        category: str,
        geography: str,
        max_events: int = 15,
    ) -> List[Dict[str, Any]]:
        """
        Main entry point — scrapes sponsors from multiple sources.
        Returns a list of raw sponsor dicts.
        """
        print(f"\n{'='*60}")
        print(f"  Sponsor Scraper -- {category} / {geography}")
        print(f"{'='*60}")

        # 1. Scrape conf.tech for tech conferences
        if category.lower() in [
            "ai", "web3", "blockchain", "climatetech", "tech",
            "devops", "cloud", "data", "ml", "software",
        ]:
            self._scrape_conftechs(category, geography, max_events)

        # 2. Scrape from conference-specific sponsor pages via search
        self._scrape_via_search(category, geography, max_events)

        # 3. Deduplicate
        self._deduplicate()

        # 4. Save raw data
        self._save_raw(category, geography)

        print(f"\n[OK] Scraped {len(self._scraped_sponsors)} unique sponsors")
        return self._scraped_sponsors

    # ── conf.tech ───────────────────────────────────────────────────────

    def _scrape_conftechs(self, category: str, geography: str, max_events: int):
        """Scrape conf.tech for tech conference listings."""
        print("\n[SCRAPE] Scraping conf.tech ...")

        # Map our categories to conf.tech topic slugs
        topic_map = {
            "ai": "artificial-intelligence",
            "ml": "machine-learning",
            "web3": "web3",
            "blockchain": "blockchain",
            "data": "data",
            "devops": "devops",
            "cloud": "cloud",
            "software": "software",
            "tech": "tech",
            "climatetech": "sustainability",
        }
        topic = topic_map.get(category.lower(), category.lower())

        # Map geography to conf.tech country codes
        geo_map = {
            "india": "india",
            "usa": "usa",
            "europe": "germany",  # as a proxy
            "singapore": "singapore",
            "uk": "uk",
        }
        geo_slug = geo_map.get(geography.lower(), geography.lower())

        url = f"https://confs.tech/{topic}?country={geo_slug}"
        html = self._fetch(url)
        if not html:
            return

        soup = BeautifulSoup(html, "lxml")

        # conf.tech renders conferences as links; extract event URLs
        event_links = []
        for a_tag in soup.select("a[href*='http']"):
            href = a_tag.get("href", "")
            if href and "confs.tech" not in href and len(event_links) < max_events:
                event_links.append(href)

        print(f"   Found {len(event_links)} event links")

        for link in event_links[:max_events]:
            self._extract_sponsors_from_event_page(
                link, category, geography
            )
            time.sleep(REQUEST_DELAY)

    # ── Search-based scraping ───────────────────────────────────────────

    def _scrape_via_search(self, category: str, geography: str, max_events: int):
        """
        Use a curated list of known conference sponsor pages to scrape.
        This is more reliable than generic search for a hackathon.
        """
        print("\n[SCRAPE] Scraping known conference sponsor pages ...")

        # Build search-friendly terms
        search_queries = self._build_search_queries(category, geography)

        for query_url in search_queries:
            html = self._fetch(query_url)
            if html:
                self._extract_sponsors_from_html(
                    html, query_url, category, geography
                )
            time.sleep(REQUEST_DELAY)

    def _build_search_queries(self, category: str, geography: str) -> List[str]:
        """
        Build a list of direct URLs to known conference sponsor pages.
        These are real, publicly accessible sponsor listing pages.
        """
        urls = []

        # ── AI / Tech conferences ───────────────────────────────────────
        if category.lower() in ["ai", "ml", "tech", "data", "software"]:
            urls.extend([
                "https://neurips.cc/Sponsors",
                "https://icml.cc/Sponsors",
                "https://iclr.cc/Sponsors",
                "https://pycon.org/sponsors/",
                "https://us.pycon.org/2025/sponsors/",
                "https://www.scipy2025.scipy.org/sponsors",
            ])

            if geography.lower() == "india":
                urls.extend([
                    "https://in.pycon.org/2024/sponsors/",
                    "https://hasgeek.com/sponsors",
                ])
            elif geography.lower() == "usa":
                urls.extend([
                    "https://us.pycon.org/2025/sponsors/",
                ])
            elif geography.lower() == "europe":
                urls.extend([
                    "https://ep2024.europython.eu/sponsors",
                ])

        # ── Web3 / Blockchain ───────────────────────────────────────────
        elif category.lower() in ["web3", "blockchain", "crypto"]:
            urls.extend([
                "https://www.ethglobal.com/sponsors",
                "https://ethcc.io/sponsors",
                "https://devcon.org/sponsors",
            ])

        # ── Music festivals ─────────────────────────────────────────────
        elif category.lower() in ["music", "music festival"]:
            urls.extend([
                "https://www.coachella.com/sponsors",
                "https://www.lollapalooza.com/sponsors",
            ])

            if geography.lower() == "india":
                urls.extend([
                    "https://www.nh7.in/sponsors",
                    "https://sunburn.in/sponsors",
                ])

        # ── Sports ──────────────────────────────────────────────────────
        elif category.lower() in ["sports", "esports"]:
            urls.extend([
                "https://www.espn.com/sponsors",
            ])

        # ── ClimateTech / Sustainability ────────────────────────────────
        elif category.lower() in ["climatetech", "sustainability", "climate"]:
            urls.extend([
                "https://www.climateweeknyc.org/sponsors",
            ])

        return urls

    # ── Sponsor extraction ──────────────────────────────────────────────

    def _extract_sponsors_from_event_page(
        self, url: str, category: str, geography: str
    ):
        """Visit an event page and try to find its sponsor page."""
        html = self._fetch(url)
        if not html:
            return

        soup = BeautifulSoup(html, "lxml")

        # Try to find a "sponsors" link on the page
        sponsor_link = None
        for a_tag in soup.find_all("a", href=True):
            href_text = a_tag.get_text(strip=True).lower()
            href_url = a_tag["href"].lower()
            if "sponsor" in href_text or "sponsor" in href_url or "partner" in href_text:
                sponsor_link = urljoin(url, a_tag["href"])
                break

        if sponsor_link:
            sponsor_html = self._fetch(sponsor_link)
            if sponsor_html:
                self._extract_sponsors_from_html(
                    sponsor_html, sponsor_link, category, geography
                )
        else:
            # Try extracting from current page
            self._extract_sponsors_from_html(html, url, category, geography)

    def _extract_sponsors_from_html(
        self, html: str, source_url: str, category: str, geography: str
    ):
        """
        Extract sponsor names from HTML.
        Looks for common patterns: sponsor sections, logo grids, partner lists.
        """
        soup = BeautifulSoup(html, "lxml")
        event_name = self._extract_event_name(soup, source_url)

        # Strategy 1: Look for sections with sponsor-related headings
        tier_keywords = {
            "title": "Title",
            "platinum": "Platinum",
            "diamond": "Diamond",
            "gold": "Gold",
            "silver": "Silver",
            "bronze": "Bronze",
            "partner": "Partner",
            "sponsor": "Partner",
            "presenting": "Title",
            "powered by": "Title",
            "supported by": "Silver",
            "community": "Community",
        }

        current_tier = "Partner"
        found_any = False

        # Look for headings that indicate sponsor tiers
        for heading in soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6", "div", "span"]):
            heading_text = heading.get_text(strip=True).lower()

            # Check if heading indicates a sponsor tier
            for keyword, tier in tier_keywords.items():
                if keyword in heading_text:
                    current_tier = tier
                    break

            # Look for sponsor names in sibling elements
            if any(kw in heading_text for kw in tier_keywords):
                container = heading.find_parent(["section", "div"])
                if container:
                    # Extract from images (sponsor logos)
                    for img in container.find_all("img"):
                        name = self._extract_name_from_img(img)
                        if name:
                            self._add_sponsor(
                                name, current_tier, event_name,
                                category, geography, source_url
                            )
                            found_any = True

                    # Extract from links
                    for a_tag in container.find_all("a", href=True):
                        name = a_tag.get_text(strip=True)
                        if name and len(name) > 1 and len(name) < 80:
                            self._add_sponsor(
                                name, current_tier, event_name,
                                category, geography, source_url
                            )
                            found_any = True

        # Strategy 2: Look for logo containers (common pattern)
        if not found_any:
            for container in soup.select(
                ".sponsors, .partners, .sponsor-grid, .logo-grid, "
                "[class*='sponsor'], [class*='partner'], [id*='sponsor'], [id*='partner']"
            ):
                for img in container.find_all("img"):
                    name = self._extract_name_from_img(img)
                    if name:
                        self._add_sponsor(
                            name, "Partner", event_name,
                            category, geography, source_url
                        )

                for a_tag in container.find_all("a"):
                    name = a_tag.get_text(strip=True)
                    if name and len(name) > 1 and len(name) < 80:
                        self._add_sponsor(
                            name, "Partner", event_name,
                            category, geography, source_url
                        )

    def _extract_name_from_img(self, img_tag) -> Optional[str]:
        """Extract a company name from an <img> tag's alt text or filename."""
        # Try alt text first
        alt = img_tag.get("alt", "").strip()
        if alt and len(alt) > 1 and len(alt) < 80:
            # Clean up common suffixes
            alt = re.sub(r"\s*(logo|image|sponsor|banner|icon).*$", "", alt, flags=re.IGNORECASE)
            if alt.strip():
                return alt.strip()

        # Try filename
        src = img_tag.get("src", "")
        if src:
            filename = os.path.basename(urlparse(src).path)
            name = os.path.splitext(filename)[0]
            name = re.sub(r"[-_]", " ", name)
            name = re.sub(r"\s*(logo|image|sponsor|banner|icon).*$", "", name, flags=re.IGNORECASE)
            if name.strip() and len(name) > 2 and len(name) < 50:
                return name.strip().title()

        return None

    def _extract_event_name(self, soup: BeautifulSoup, url: str) -> str:
        """Get the event name from the page title or URL."""
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text(strip=True)
            # Clean up common suffixes
            title = re.split(r"\s*[|–—-]\s*", title)[0].strip()
            if title and len(title) < 100:
                return title

        # Fallback to domain name
        parsed = urlparse(url)
        return parsed.netloc.replace("www.", "")

    # ── Helpers ─────────────────────────────────────────────────────────

    def _add_sponsor(
        self,
        name: str,
        tier: str,
        event_name: str,
        category: str,
        geography: str,
        source_url: str,
    ):
        """Add a sponsor record to the scraped list."""
        # Basic cleaning
        name = name.strip()
        if not name or len(name) < 2:
            return

        # Skip generic/nav text
        skip_words = {
            "home", "about", "contact", "menu", "login", "sign up",
            "register", "back", "next", "previous", "close", "submit",
            "search", "privacy", "terms", "cookie", "sponsor us",
        }
        if name.lower() in skip_words:
            return

        self._scraped_sponsors.append({
            "company_name": name,
            "tier": tier,
            "event_name": event_name,
            "event_category": category,
            "geography": geography,
            "year": 2025,
            "source_url": source_url,
        })

    def _deduplicate(self):
        """Remove duplicates by (company_name, event_name) pair."""
        seen = set()
        unique = []
        for entry in self._scraped_sponsors:
            key = (entry["company_name"].lower(), entry["event_name"].lower())
            if key not in seen:
                seen.add(key)
                unique.append(entry)
        self._scraped_sponsors = unique

    def _save_raw(self, category: str, geography: str):
        """Save raw scraped data to JSON."""
        filename = f"sponsors_{category.lower()}_{geography.lower()}.json"
        filepath = os.path.join(SCRAPED_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self._scraped_sponsors, f, indent=2, ensure_ascii=False)
        print(f"   [SAVED] Saved to {filepath}")

    def _fetch(self, url: str) -> Optional[str]:
        """Fetch a URL with retries."""
        for attempt in range(MAX_RETRIES):
            try:
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    return resp.text
                elif resp.status_code == 403:
                    print(f"   [WARN] 403 Forbidden: {url}")
                    return None
                elif resp.status_code == 404:
                    print(f"   [WARN] 404 Not Found: {url}")
                    return None
            except requests.exceptions.RequestException as e:
                print(f"   [WARN] Request failed ({attempt+1}/{MAX_RETRIES}): {e}")
                time.sleep(REQUEST_DELAY * (attempt + 1))

        return None


def build_sponsor_database(
    scraped_data: List[Dict[str, Any]],
) -> List[Sponsor]:
    """
    Convert raw scraped records into consolidated Sponsor objects.
    Groups by company name and aggregates past sponsorships.
    """
    company_map: Dict[str, Sponsor] = {}

    for rec in scraped_data:
        name = rec["company_name"]
        key = name.lower().strip()

        if key not in company_map:
            company_map[key] = Sponsor(
                company_name=name,
                industry="",           # will be enriched by LLM
                headquarters="",
                company_size="",
                description="",
                website=None,
                past_sponsorships=[],
                marketing_focus=[],
            )

        company_map[key].past_sponsorships.append(
            PastSponsorship(
                event_name=rec.get("event_name", ""),
                event_category=rec.get("event_category", ""),
                year=rec.get("year", 2025),
                tier=rec.get("tier", "Partner"),
                geography=rec.get("geography", ""),
                event_url=rec.get("source_url"),
            )
        )

    return list(company_map.values())
