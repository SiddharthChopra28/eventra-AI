import json
from dataclasses import asdict

from .models import Artist
from .ranker import ArtistRanker
from .sources import BandsInTownScraper, SongkickScraper


class ArtistAgent:
    def __init__(self):
        self.songkick = SongkickScraper()
        self.bandsintown = BandsInTownScraper()
        self.ranker = ArtistRanker()

    def _deduplicate(self, artists: list[Artist]) -> list[Artist]:
        seen: dict[str, Artist] = {}
        for artist in artists:
            key = artist.name.lower().strip()
            if key not in seen:
                seen[key] = artist
                continue
            current = seen[key]
            if (artist.popularity_score or 0) > (current.popularity_score or 0):
                seen[key] = artist
        return list(seen.values())

    def run(
        self,
        city: str,
        event_type: str = "music",
        audience_size: int = 500,
        top_n: int = 10,
    ) -> dict:
        print(f"\n{'=' * 60}")
        print(f"[ArtistAgent] {city} | {event_type} | {audience_size} people")
        print(f"{'=' * 60}")

        artists = self.songkick.fetch(city) + self.bandsintown.fetch(city)
        artists = self._deduplicate(artists)
        ranked = self.ranker.score(artists, event_type, audience_size)
        top = ranked[:top_n]
        self._print_table(top)

        return {
            "agent": "ArtistAgent",
            "query": {"city": city, "event_type": event_type, "audience_size": audience_size},
            "total_found": len(artists),
            "sources_used": list({artist.source for artist in artists}),
            "top_artists": [asdict(artist) for artist in top],
        }

    def _print_table(self, artists: list[Artist]) -> None:
        print(f"\n{'#':<4} {'Artist':<32} {'Source':<14} {'Score':>6}  Popularity")
        print("-" * 72)
        for index, artist in enumerate(artists, 1):
            popularity = f"{artist.popularity_score:.2f}" if artist.popularity_score else "-"
            print(
                f"{index:<4} {artist.name[:31]:<32} {artist.source:<14} "
                f"{artist.relevance_score:>6.3f}  {popularity}"
            )

    def save(self, output: dict, path: str) -> None:
        with open(path, "w", encoding="utf-8") as file:
            json.dump(output, file, indent=2, ensure_ascii=False)
        print(f"\n[ArtistAgent] Saved {len(output['top_artists'])} artists -> {path}")
