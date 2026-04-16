from .models import Venue


class VenueRanker:
    TYPE_KEYWORDS = {
        "tech": ["conference", "convention", "tech", "innovation", "exhibition"],
        "music": ["auditorium", "hall", "amphitheatre", "stadium", "arena"],
        "startup": ["coworking", "hub", "incubator", "innovation", "space"],
        "sports": ["stadium", "arena", "ground", "complex", "indoor"],
        "web3": ["conference", "convention", "hub", "innovation"],
        "climate": ["conference", "convention", "exhibition", "civic"],
    }

    def score(self, venues: list[Venue], required_capacity: int, event_type: str) -> list[Venue]:
        keywords = self.TYPE_KEYWORDS.get(event_type.lower(), ["conference", "hall", "venue"])

        for venue in venues:
            score = 0.0

            if venue.capacity:
                if venue.capacity >= required_capacity:
                    score += 0.40
                else:
                    score += 0.40 * (venue.capacity / max(required_capacity, 1))
            else:
                score += 0.15

            blob = (venue.name + " " + venue.venue_type).lower()
            score += 0.30 if any(keyword in blob for keyword in keywords) else 0.05

            if venue.website:
                score += 0.05
            if venue.phone:
                score += 0.04
            if venue.rating:
                score += 0.06 * (venue.rating / 5.0)
            if venue.amenities:
                score += min(0.05, len(venue.amenities) * 0.012)

            score += {"VenueLook": 0.10, "OpenStreetMap": 0.08}.get(venue.source, 0.05)

            venue.relevance_score = round(min(score, 1.0), 3)

        return sorted(venues, key=lambda item: item.relevance_score, reverse=True)
