import os


VENUELOOK_BASE_URL = os.getenv("VENUELOOK_BASE_URL", "https://www.venuelook.com").strip()

CITY_COORDS: dict[str, tuple[float, float]] = {
    "bangalore": (12.9716, 77.5946),
    "mumbai": (19.0760, 72.8777),
    "delhi": (28.6139, 77.2090),
    "hyderabad": (17.3850, 78.4867),
    "pune": (18.5204, 73.8567),
    "chennai": (13.0827, 80.2707),
    "new york": (40.7128, -74.0060),
    "san francisco": (37.7749, -122.4194),
    "singapore": (1.3521, 103.8198),
    "london": (51.5074, -0.1278),
    "berlin": (52.5200, 13.4050),
}

DEFAULT_COORDS = (20.5937, 78.9629)
