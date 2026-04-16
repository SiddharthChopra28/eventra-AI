import re
from urllib.parse import quote_plus

import httpx

from .models import Artist


class SongkickScraper:
    BASE_URL = "https://www.songkick.com"

    METRO_LINK_RE = re.compile(r"/metro-areas/\d+-[a-z0-9-]+", flags=re.IGNORECASE)
    ARTIST_LINK_RE = re.compile(r'href="(?P<href>/artists/[^"]+)"[^>]*>(?P<name>.*?)</a>', flags=re.IGNORECASE)
    UPCOMING_ARTIST_RE = re.compile(r"\*\*(?P<name>[^*]+)\*\*", flags=re.IGNORECASE)
    EVENT_BLOCK_RE = re.compile(
        r'<li[^>]*class="event-listings-element"[^>]*>(?P<block>.*?)</li>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    EVENT_ARTIST_RE = re.compile(r"<strong>(?P<name>[^<]+)</strong>", flags=re.IGNORECASE)
    EVENT_VENUE_RE = re.compile(
        r'<p[^>]*class="venue-name"[^>]*>\s*<a[^>]*>(?P<venue>.*?)</a>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    TAG_RE = re.compile(r"<[^>]+>")

    @staticmethod
    def _clean(text: str) -> str:
        cleaned = SongkickScraper.TAG_RE.sub(" ", text)
        return " ".join(cleaned.split()).strip()

    def fetch(self, city: str) -> list[Artist]:
        url = f"{self.BASE_URL}/search?query={quote_plus(city)}"
        headers = {"User-Agent": "artist-agent/1.0"}
        try:
            response = httpx.get(url, headers=headers, timeout=30, follow_redirects=True)
            response.raise_for_status()
        except Exception as error:
            print(f"[Songkick] Failed: {type(error).__name__}")
            return []

        artists: list[Artist] = []

        # Try metro page if we can resolve a city-specific metro area.
        metro_links = [m.group(0) for m in self.METRO_LINK_RE.finditer(response.text.lower())]
        city_slug = city.strip().lower().replace(" ", "-")
        selected_metro = next((m for m in metro_links if city_slug in m), None)
        if selected_metro:
            metro_url = f"{self.BASE_URL}{selected_metro}"
            try:
                metro_page = httpx.get(metro_url, headers=headers, timeout=30, follow_redirects=True)
                metro_page.raise_for_status()
                html = metro_page.text
                for block_match in self.EVENT_BLOCK_RE.finditer(html):
                    block = block_match.group("block")
                    name_match = self.EVENT_ARTIST_RE.search(block)
                    if not name_match:
                        continue
                    name = self._clean(name_match.group("name"))
                    if not name:
                        continue
                    venue_match = self.EVENT_VENUE_RE.search(block)
                    venue = self._clean(venue_match.group("venue")) if venue_match else None
                    artists.append(
                        Artist(
                            name=name,
                            city=city,
                            genre="music",
                            source="Songkick",
                            profile_url=metro_url,
                            notes=venue,
                            tags=["upcoming", "city_concert"],
                            popularity_score=3.2,
                        )
                    )

                # Fallback parser for markdown-like rendering in non-browser fetchers.
                for match in self.UPCOMING_ARTIST_RE.finditer(metro_page.text):
                    name = self._clean(match.group("name"))
                    if not name or len(name) < 2 or "upcoming" in name.lower():
                        continue
                    artists.append(
                        Artist(
                            name=name,
                            city=city,
                            genre="music",
                            source="Songkick",
                            profile_url=metro_url,
                            tags=["upcoming", "city_concert"],
                            popularity_score=3.0,
                        )
                    )
            except Exception as error:
                print(f"[Songkick] Metro fetch failed: {type(error).__name__}")

        # Final fallback to generic artist links if metro parsing yielded nothing.
        if not artists:
            for match in self.ARTIST_LINK_RE.finditer(response.text):
                name = self._clean(match.group("name"))
                href = match.group("href")
                if not name or "concert tickets" in name.lower():
                    continue
                artists.append(
                    Artist(
                        name=name,
                        city=city,
                        genre="music",
                        source="Songkick",
                        profile_url=f"{self.BASE_URL}{href}",
                        tags=["live", "touring", "search"],
                    )
                )

        # Fallback to popular artist section on homepage if search extraction is empty.
        if not artists:
            try:
                home = httpx.get(self.BASE_URL, headers=headers, timeout=30).text
                home_names = re.findall(r"#####\s+([^\n#][^\n]+)", home)
                for raw in home_names[:20]:
                    name = self._clean(raw)
                    if not name or "See all" in name:
                        continue
                    artists.append(
                        Artist(
                            name=name,
                            city=city,
                            genre="music",
                            source="Songkick",
                            tags=["popular"],
                        )
                    )
            except Exception:
                pass

        deduped: dict[str, Artist] = {}
        for artist in artists:
            key = artist.name.lower()
            if key not in deduped:
                deduped[key] = artist
        parsed = list(deduped.values())
        print(f"[Songkick] Found {len(parsed)} artists for {city}")
        return parsed


class BandsInTownScraper:
    def fetch(self, city: str) -> list[Artist]:
        # Bandsintown currently blocks server-side scraping from this runtime (Cloudflare 403).
        # Return empty list gracefully so the pipeline can still run on Songkick data.
        print(f"[Bandsintown] Skipped for {city} (Cloudflare-protected in this runtime)")
        return []
