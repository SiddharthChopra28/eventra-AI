from .models import Artist


class ArtistRanker:
    EVENT_TYPE_TAGS = {
        "music": ["live", "touring", "popular", "tracked_artist"],
    }

    def score(self, artists: list[Artist], event_type: str, audience_size: int) -> list[Artist]:
        tags = self.EVENT_TYPE_TAGS.get(event_type.lower(), ["live", "music"])
        for artist in artists:
            score = 0.0
            if artist.popularity_score:
                score += min(0.45, (artist.popularity_score / 5.0) * 0.45)
            else:
                score += 0.18

            tag_blob = " ".join(artist.tags).lower()
            score += 0.25 if any(tag in tag_blob for tag in tags) else 0.08

            if artist.profile_url:
                score += 0.10
            if artist.notes:
                score += 0.05

            # Prefer more established profiles for larger audiences.
            if audience_size >= 1000:
                score += 0.15 if artist.popularity_score and artist.popularity_score >= 3.5 else 0.05
            elif audience_size >= 300:
                score += 0.10
            else:
                score += 0.06

            score += {"Songkick": 0.12, "Bandsintown": 0.10}.get(artist.source, 0.05)
            artist.relevance_score = round(min(score, 1.0), 3)

        return sorted(artists, key=lambda item: item.relevance_score, reverse=True)
