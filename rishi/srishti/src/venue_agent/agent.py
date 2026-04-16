import json
from dataclasses import asdict

from .config import CITY_COORDS, DEFAULT_COORDS
from .models import Venue
from .ranker import VenueRanker
from .sources import NominatimClient, OverpassScraper, VenueLookScraper


class VenueAgent:
    def __init__(self):
        self.osm = OverpassScraper()
        self.venuelook = VenueLookScraper()
        self.geo = NominatimClient()
        self.ranker = VenueRanker()

    def _deduplicate(self, venues: list[Venue]) -> list[Venue]:
        seen: dict[str, Venue] = {}
        for venue in venues:
            key = venue.name.lower().strip()[:35]
            if key not in seen or (venue.capacity or 0) > (seen[key].capacity or 0):
                seen[key] = venue
        return list(seen.values())

    def _resolve_coordinates(self, city: str) -> tuple[float, float]:
        city_lower = city.lower().strip()
        if city_lower in CITY_COORDS:
            return CITY_COORDS[city_lower]
        geocoded = self.geo.geocode_city(city)
        if geocoded:
            return geocoded
        return DEFAULT_COORDS

    def run(
        self,
        city: str,
        event_type: str = "tech",
        audience_size: int = 500,
        top_n: int = 10,
    ) -> dict:
        lat, lon = self._resolve_coordinates(city)
        print(f"\n{'=' * 60}")
        print(f"[VenueAgent] {city} | {event_type} | {audience_size} people")
        print(f"{'=' * 60}")

        venues = self.osm.fetch(city) + self.venuelook.fetch(city)

        venues = self._deduplicate(venues)
        ranked = self.ranker.score(venues, audience_size, event_type)
        top = ranked[:top_n]
        self._print_table(top)

        return {
            "agent": "VenueAgent",
            "query": {"city": city, "event_type": event_type, "audience_size": audience_size},
            "total_found": len(venues),
            "sources_used": list({venue.source for venue in venues}),
            "top_venues": [asdict(venue) for venue in top],
        }

    def _print_table(self, venues: list[Venue]) -> None:
        print(f"\n{'#':<4} {'Venue Name':<40} {'Cap':>6}  {'Score':>6}  Rating")
        print("-" * 72)
        for index, venue in enumerate(venues, 1):
            capacity = f"{venue.capacity:,}" if venue.capacity else "  n/a"
            rating = f"{venue.rating:.1f}*" if venue.rating else "  -"
            print(f"{index:<4} {venue.name[:39]:<40} {capacity:>6}  {venue.relevance_score:>6.3f}  {rating}")

    def save(self, output: dict, path: str) -> None:
        with open(path, "w", encoding="utf-8") as file:
            json.dump(output, file, indent=2, ensure_ascii=False)
        print(f"\n[VenueAgent] Saved {len(output['top_venues'])} venues -> {path}")
