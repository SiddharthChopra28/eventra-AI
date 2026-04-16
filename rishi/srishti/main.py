import argparse

from src.venue_agent import VenueAgent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Venue Agent - Conference AI System")
    parser.add_argument("--city", required=True, help="City name, e.g. bangalore")
    parser.add_argument(
        "--event-type",
        default="tech",
        choices=["tech", "music", "sports", "startup", "web3", "climate"],
        help="Type of event",
    )
    parser.add_argument("--audience-size", type=int, default=500, help="Expected audience size")
    parser.add_argument("--top-n", type=int, default=8, help="Number of venues to return")
    parser.add_argument(
        "--output",
        default="venues_output.json",
        help="Output JSON file path",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    agent = VenueAgent()
    result = agent.run(
        city=args.city,
        event_type=args.event_type,
        audience_size=args.audience_size,
        top_n=args.top_n,
    )
    agent.save(result, args.output)
