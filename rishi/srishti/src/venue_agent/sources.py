import time
import re
from typing import Optional

import httpx

from .config import VENUELOOK_BASE_URL
from .models import Venue


class NominatimClient:
    BASE_URL = "https://nominatim.openstreetmap.org/search"

    def geocode_city(self, city: str) -> Optional[tuple[float, float]]:
        params = {"q": city, "format": "json", "limit": 1}
        headers = {"User-Agent": "venue-agent/1.0"}
        try:
            response = httpx.get(self.BASE_URL, params=params, headers=headers, timeout=20)
            response.raise_for_status()
            data = response.json()
            if not data:
                return None
            return float(data[0]["lat"]), float(data[0]["lon"])
        except Exception:
            return None


class OverpassScraper:
    ENDPOINTS = [
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass.openstreetmap.ru/api/interpreter",
    ]
    OSM_TAGS = [
        '"amenity"="conference_centre"',
        '"amenity"="events_venue"',
        '"amenity"="exhibition_centre"',
        '"amenity"="auditorium"',
        '"building"="civic"',
        '"leisure"="stadium"',
    ]

    def build_query(self, city: str) -> str:
        tag_lines = "\n".join(
            [
                f'  node[{tag}](area.searchArea);\n  way[{tag}](area.searchArea);'
                for tag in self.OSM_TAGS
            ]
        )
        return f"""
[out:json][timeout:30];
area[name="{city}"][admin_level~"4|6|8"]->.searchArea;
(
{tag_lines}
);
out body center 50;
"""

    def parse_element(self, element: dict, city: str) -> Optional[Venue]:
        tags = element.get("tags", {})
        name = tags.get("name") or tags.get("name:en")
        if not name:
            return None

        lat = element.get("lat") or element.get("center", {}).get("lat")
        lon = element.get("lon") or element.get("center", {}).get("lon")
        if not lat or not lon:
            return None

        raw_capacity = tags.get("capacity")
        capacity = int(raw_capacity) if raw_capacity and str(raw_capacity).isdigit() else None

        addr_parts = [
            tags.get("addr:housenumber", ""),
            tags.get("addr:street", ""),
            tags.get("addr:suburb", ""),
            tags.get("addr:city", city),
        ]
        address = ", ".join(part for part in addr_parts if part).strip(", ") or city

        amenities: list[str] = []
        if tags.get("parking") in ("yes", "surface"):
            amenities.append("parking")
        if tags.get("wheelchair") == "yes":
            amenities.append("wheelchair_access")
        if tags.get("internet_access"):
            amenities.append("wifi")
        if tags.get("air_conditioning") == "yes":
            amenities.append("ac")

        return Venue(
            name=name,
            address=address,
            city=city,
            lat=float(lat),
            lon=float(lon),
            capacity=capacity,
            venue_type=tags.get("amenity") or tags.get("building") or "venue",
            source="OpenStreetMap",
            website=tags.get("website") or tags.get("url"),
            phone=tags.get("phone") or tags.get("contact:phone"),
            amenities=amenities,
        )

    def fetch(self, city: str) -> list[Venue]:
        query = self.build_query(city)
        for endpoint in self.ENDPOINTS:
            try:
                response = httpx.post(endpoint, data={"data": query}, timeout=40)
                if response.status_code == 200:
                    elements = response.json().get("elements", [])
                    venues = [v for e in elements if (v := self.parse_element(e, city))]
                    print(f"[OSM] Found {len(venues)} venues in {city}")
                    return venues
            except Exception as error:
                print(f"[OSM] {endpoint} failed: {type(error).__name__}")
                time.sleep(0.3)
        print("[OSM] All endpoints unavailable")
        return []


class VenueLookScraper:
    TABLE_ROW_RE = re.compile(
        r"<tr><td[^>]*>(.*?)</td><td[^>]*>(.*?)</td><td[^>]*>(.*?)</td></tr>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    TAG_RE = re.compile(r"<[^>]+>")
    CAPACITY_RE = re.compile(r"(?:upto\s*)?(\d+)(?:\s*-\s*(\d+))?\s*guests?", flags=re.IGNORECASE)

    @staticmethod
    def _clean_text(raw: str) -> str:
        text = VenueLookScraper.TAG_RE.sub(" ", raw)
        text = (
            text.replace("&amp;", "&")
            .replace("&quot;", '"')
            .replace("&#x27;", "'")
            .replace("&#39;", "'")
            .replace("&nbsp;", " ")
        )
        return " ".join(text.split()).strip()

    @staticmethod
    def _extract_capacity(raw_capacity: str) -> Optional[int]:
        match = VenueLookScraper.CAPACITY_RE.search(raw_capacity.lower())
        if not match:
            return None
        low = int(match.group(1))
        high = int(match.group(2)) if match.group(2) else low
        return max(low, high)

    @staticmethod
    def _split_type_and_name(raw_name: str) -> tuple[str, str]:
        lowered = raw_name.lower()
        if " of " in lowered:
            left, right = raw_name.split(" of ", 1)
            return left.strip().replace(" ", "_"), right.strip()
        return "venue", raw_name.strip()

    def fetch(self, city: str) -> list[Venue]:
        city_slug = city.strip().lower().replace(" ", "-")
        url = f"{VENUELOOK_BASE_URL.rstrip('/')}/{city_slug}"
        headers = {"User-Agent": "venue-agent/1.0"}

        try:
            response = httpx.get(url, headers=headers, timeout=30, follow_redirects=True)
            response.raise_for_status()
        except Exception as error:
            print(f"[VenueLook] Failed for {city}: {type(error).__name__}")
            return []

        rows = self.TABLE_ROW_RE.findall(response.text)
        venues: list[Venue] = []
        for raw_name, raw_capacity, raw_price in rows:
            cleaned_name = self._clean_text(raw_name)
            cleaned_capacity = self._clean_text(raw_capacity)
            cleaned_price = self._clean_text(raw_price)
            if not cleaned_name or "guest" not in cleaned_capacity.lower():
                continue

            venue_type, venue_name = self._split_type_and_name(cleaned_name)
            venues.append(
                Venue(
                    name=venue_name.title(),
                    address=city.title(),
                    city=city,
                    lat=0.0,
                    lon=0.0,
                    capacity=self._extract_capacity(cleaned_capacity),
                    venue_type=venue_type,
                    source="VenueLook",
                    website=url,
                    price_per_day=cleaned_price,
                )
            )

        deduped: dict[str, Venue] = {}
        for venue in venues:
            key = f"{venue.name.lower()}::{venue.venue_type}"
            if key not in deduped:
                deduped[key] = venue

        parsed = list(deduped.values())
        print(f"[VenueLook] Found {len(parsed)} venues in {city}")
        return parsed
