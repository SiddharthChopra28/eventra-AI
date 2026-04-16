# 🎯 Exhibitor Agent

A production-grade AI agent that uses historical event data to generate **intelligent, data-backed exhibitor recommendations**.

---

## Architecture

```
exhibitor_agent/
├── main.py           # FastAPI app + routes
├── agent.py          # Orchestrator — wires the full pipeline
├── data_loader.py    # JSON ingestion → Pandas DataFrames (with caching)
├── similarity.py     # Event similarity engine (category + geo + audience)
├── scoring.py        # Weighted relevance scoring + normalization (0–100)
├── clustering.py     # Rule-based + KMeans clustering
├── insights.py       # Data-driven insight generation
├── models.py         # Pydantic request/response models
├── sample_data/
│   └── events.json   # 12 historical events with exhibitors
└── requirements.txt
```

---

## Intelligence Pipeline

```
Query (category, geography, audience_size)
        │
        ▼
 Step 1: Similar Event Filtering
   ├── Category similarity  (weight: 0.50)
   ├── Geography match      (weight: 0.30)
   └── Audience proximity   (weight: 0.20)
        │
        ▼
 Step 2: Exhibitor Extraction
   └── Pool all exhibitors from similar events
        │
        ▼
 Step 3: Pattern Learning
   ├── Frequency distribution
   ├── Category-wise dominance
   └── Type trends (Startup vs Enterprise vs Tool/Platform)
        │
        ▼
 Step 4: Relevance Scoring
   Score = 0.4×Frequency + 0.3×Category + 0.2×Geography + 0.1×AudienceFit
   └── Normalised 0–100
        │
        ▼
 Step 5: Clustering
   ├── Rule-based: Startups / Enterprises / Tools+Platforms / Others
   └── ML KMeans (optional, sklearn)
        │
        ▼
 Step 6: Ranked Recommendations + Insights
```

---

## Quick Start

### 1. Install dependencies
```bash
cd exhibitor_agent
pip install -r requirements.txt
```

### 2. Run the API
```bash
python main.py
# or
uvicorn main:app --reload --port 8000
```

### 3. Make a recommendation request
```bash
curl -X POST http://localhost:8000/recommend \
  -H "Content-Type: application/json" \
  -d '{"category": "AI", "geography": "India", "audience_size": 3000}'
```

### 4. Swagger UI
Open: http://localhost:8000/docs

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Agent health + dataset stats |
| GET | `/events/summary` | Dataset summary |
| POST | `/recommend` | Generate recommendations |

### Request Schema
```json
{
  "category": "AI",
  "geography": "India",
  "audience_size": 3000,
  "top_n": 10,
  "min_score": 0.0
}
```

### Response Schema
```json
{
  "query": {...},
  "similar_events_used": [...],
  "recommended_exhibitors": [
    {
      "name": "Google Cloud",
      "type": "Enterprise",
      "score": 95.4,
      "reason": "Appeared in 6 similar events; strong presence in India; established enterprise exhibitor.",
      "appeared_count": 6,
      "appeared_in_events": ["EVT001", "EVT002", ...]
    }
  ],
  "clusters": {
    "Enterprises": {"exhibitors": [...], "count": 5, "percentage": 45.5},
    "Startups": {...},
    "Tools/Platforms": {...},
    "Others": {...}
  },
  "insights": [
    "45.5% of exhibitors in similar AI events in India are Enterprises.",
    "Top repeat exhibitors across similar events: Google Cloud, Hugging Face, ..."
  ],
  "metadata": {
    "elapsed_seconds": 0.012,
    "similar_events_count": 7,
    "candidate_pool_size": 18
  }
}
```

---

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `EVENTS_DATA_PATH` | `sample_data/events.json` | Path to your events JSON |

---

## Extending the Agent

### Plug into a Multi-Agent Orchestrator
```python
from agent import ExhibitorAgent
from models import RecommendationRequest

agent = ExhibitorAgent()
agent.load_data()

# Call from orchestrator
result = agent.run(RecommendationRequest(
    category="AI",
    geography="India",
    audience_size=3000
))
```

### Swap in your own dataset
Replace `sample_data/events.json` with any JSON matching:
```json
[
  {
    "event_id": "...",
    "event_name": "...",
    "category": "...",
    "location": "...",
    "country": "...",
    "audience_size": 2000,
    "year": 2024,
    "exhibitors": [
      {"name": "...", "type": "Enterprise|Startup|Tool|Platform|Others"}
    ]
  }
]
```

---

## Scoring Formula

```
Relevance Score =
  (0.40 × Frequency Score)       # How often in similar events (log-normalised)
  (0.30 × Category Match Score)  # Fraction of appearances in target category
  (0.20 × Geography Match Score) # Fraction of appearances in target geography
  (0.10 × Audience Fit Score)    # Gaussian fit to target audience size

Final score normalised 0–100.
```