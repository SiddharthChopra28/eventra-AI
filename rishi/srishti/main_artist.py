import argparse

from src.artist_agent import ArtistAgent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Artist Agent - Music Artist Discovery")
    parser.add_argument("--city", required=True, help="City name, e.g. bangalore")
    parser.add_argument(
        "--event-type",
        default="music",
        choices=["music"],
        help="Type of event",
    )
    parser.add_argument("--audience-size", type=int, default=500, help="Expected audience size")
    parser.add_argument("--top-n", type=int, default=8, help="Number of artists to return")
    parser.add_argument(
        "--output",
        default="artists_output.json",
        help="Output JSON file path",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    agent = ArtistAgent()
    result = agent.run(
        city=args.city,
        event_type=args.event_type,
        audience_size=args.audience_size,
        top_n=args.top_n,
    )
    agent.save(result, args.output)
