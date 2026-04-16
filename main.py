"""
main.py - FastAPI application for the Exhibitor Agent.

Startup: loads data once, caches agent instance.
Routes:
  GET  /health          - agent health check
  POST /recommend       - main recommendation endpoint
  GET  /events/summary  - dataset summary
  GET  /docs            - Swagger UI (built-in)
"""
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from agent import ExhibitorAgent
from models import (
    RecommendationRequest,
    RecommendationResponse,
    HealthResponse,
)

# ── Logging ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

# ── Global agent instance ─────────────────────────────────────────────
_agent: ExhibitorAgent = None  # type: ignore


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _agent
    data_path = os.environ.get("EVENTS_DATA_PATH", None)
    _agent = ExhibitorAgent(data_path=data_path, use_ml_clustering=True)
    _agent.load_data()
    logger.info("ExhibitorAgent ready.")
    yield
    logger.info("Shutting down ExhibitorAgent.")


# ── App ───────────────────────────────────────────────────────────────
app = FastAPI(
    title="Exhibitor Agent API",
    description=(
        "Production-grade AI agent that analyses historical event data "
        "to recommend and rank exhibitors for your next event."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Exception handler ─────────────────────────────────────────────────
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": str(exc)})


# ── Routes ────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse, tags=["Utility"])
async def health():
    """Returns agent health and dataset statistics."""
    return HealthResponse(**_agent.health())


@app.get("/events/summary", tags=["Utility"])
async def events_summary():
    """Returns a summary of the loaded event dataset."""
    if not _agent._loaded:
        raise HTTPException(status_code=503, detail="Data not loaded yet.")
    return _agent.loader.summary()


@app.post(
    "/recommend",
    response_model=RecommendationResponse,
    tags=["Recommendations"],
    summary="Generate exhibitor recommendations",
)
async def recommend(request: RecommendationRequest):
    """
    Accepts an event descriptor and returns ranked exhibitor recommendations
    derived entirely from historical event data signals.

    **Example input:**
    ```json
    {
      "category": "AI",
      "geography": "India",
      "audience_size": 3000
    }
    ```
    """
    try:
        response = _agent.run(request)
        return response
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Error in /recommend: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal agent error.")


# ── Dev entrypoint ────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)