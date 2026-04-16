# Venue Agent - Conference AI System

Find and rank event venues using free data sources:
- Overpass API (OpenStreetMap venues)
- Nominatim (city geocoding)
- VenueLook city listings (web scraping)

## Project structure

```
srishti/
├─ main.py
├─ requirements.txt
├─ .env.example
└─ src/
   └─ venue_agent/
      ├─ __init__.py
      ├─ agent.py
      ├─ config.py
      ├─ models.py
      ├─ ranker.py
      └─ sources.py
```

## Setup

1) Create and activate a virtual environment:

Windows PowerShell:
```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2) Install dependencies:
```
pip install -r requirements.txt
```

3) Optional configuration:
- Copy `.env.example` values into your terminal environment.
- Example in PowerShell:
```
$env:VENUELOOK_BASE_URL="https://www.venuelook.com"
```

## Run

Basic:
```
python main.py --city bangalore --event-type tech --audience-size 500 --top-n 8 --output bangalore_tech_venues.json
```

More examples:
```
python main.py --city mumbai --event-type music --audience-size 2000 --top-n 5 --output mumbai_music_venues.json
python main.py --city singapore --event-type startup --audience-size 300 --top-n 10 --output singapore_startup_venues.json
```

## Notes

- Venue results come from VenueLook and OpenStreetMap.
