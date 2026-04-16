"""
agent.py - The Exhibitor Agent orchestrator.

Wires together: DataLoader → Similarity → Scoring → Clustering → Insights
Designed to be pluggable into a multi-agent orchestrator via the `run()` method.
"""
import logging
import time
from functools import lru_cache
from typing import Optional, Dict, Any, List

import pandas as pd

from data_loader import DataLoader
from similarity import compute_event_similarities
from scoring import score_exhibitors, build_reason
from clustering import cluster_exhibitors
from insights import generate_insights
from models import (
    RecommendationRequest,
    RecommendationResponse,
    ExhibitorRecommendation,
    ClusterInfo,
    SimilarEvent,
    ExhibitorType,
)

logger = logging.getLogger(__name__)

# Minimum similarity threshold to consider an event "similar"
MIN_EVENT_SIMILARITY = 0.20
# If fewer than this many similar events are found, relax the threshold
FALLBACK_MIN_EVENTS = 3


class ExhibitorAgent:
    """
    Production-grade exhibitor recommendation agent.

    Usage:
        agent = ExhibitorAgent()
        agent.load_data()
        response = agent.run(request)
    """

    VERSION = "1.0.0"

    def __init__(self, data_path: Optional[str] = None, use_ml_clustering: bool = True):
        self.loader = DataLoader(data_path)
        self.use_ml_clustering = use_ml_clustering
        self._loaded = False
        logger.info(f"ExhibitorAgent v{self.VERSION} initialised.")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def load_data(self) -> "ExhibitorAgent":
        """Load dataset. Call once at startup."""
        self.loader.load()
        self._loaded = True
        summary = self.loader.summary()
        logger.info(f"Data loaded: {summary}")
        return self

    def health(self) -> Dict[str, Any]:
        summary = self.loader.summary() if self._loaded else {}
        return {
            "status": "ok" if self._loaded else "not_loaded",
            "version": self.VERSION,
            "dataset_loaded": self._loaded,
            "total_events": summary.get("total_events", 0),
            "total_unique_exhibitors": summary.get("unique_exhibitors", 0),
        }

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------

    def run(self, request: RecommendationRequest) -> RecommendationResponse:
        if not self._loaded:
            raise RuntimeError("Call load_data() before run().")

        t0 = time.perf_counter()
        logger.info(f"Running agent for: {request}")

        events_df = self.loader.events_df
        exhibitors_df = self.loader.exhibitors_df

        # ── Step 1: Find similar events ──────────────────────────────
        scored_events = compute_event_similarities(
            query_category=request.category,
            query_geography=request.geography,
            query_audience=request.audience_size,
            events_df=events_df,
        )

        # Pick threshold dynamically
        threshold = MIN_EVENT_SIMILARITY
        similar = scored_events[scored_events["similarity_score"] >= threshold]
        if len(similar) < FALLBACK_MIN_EVENTS:
            # Relax threshold gradually
            for t in [0.15, 0.10, 0.05]:
                similar = scored_events[scored_events["similarity_score"] >= t]
                if len(similar) >= FALLBACK_MIN_EVENTS:
                    logger.info(f"Relaxed similarity threshold to {t}")
                    break

        if similar.empty:
            similar = scored_events.head(FALLBACK_MIN_EVENTS)
            logger.warning("Using top-N events regardless of similarity score.")

        similar_event_ids = similar["event_id"].tolist()
        logger.info(f"Selected {len(similar_event_ids)} similar events.")

        # ── Step 2: Extract exhibitor pool from similar events ────────
        exhibitor_pool = exhibitors_df[exhibitors_df["event_id"].isin(similar_event_ids)].copy()

        if exhibitor_pool.empty:
            logger.warning("Exhibitor pool is empty after filtering.")
            return self._empty_response(request, similar, events_df)

        # ── Step 3 + 4: Pattern learning + scoring ───────────────────
        scored_df = score_exhibitors(
            exhibitor_pool=exhibitor_pool,
            all_exhibitor_rows=exhibitors_df,
            query_category=request.category,
            query_geography=request.geography,
            query_audience=request.audience_size,
            similar_event_ids=similar_event_ids,
        )

        # Apply min score filter
        if request.min_score and request.min_score > 0:
            scored_df = scored_df[scored_df["score"] >= request.min_score]

        top_df = scored_df.head(request.top_n or 10)

        # ── Step 5: Clustering ────────────────────────────────────────
        clusters_raw = cluster_exhibitors(scored_df, use_ml=self.use_ml_clustering)

        # ── Step 6: Build recommendations ────────────────────────────
        recommendations = []
        for _, row in top_df.iterrows():
            reason = build_reason(row, events_df, request.geography)
            recommendations.append(
                ExhibitorRecommendation(
                    name=row["exhibitor_name"],
                    type=ExhibitorType(row["exhibitor_type"]),
                    score=round(float(row["score"]), 2),
                    frequency_score=round(float(row["frequency_score"]), 4),
                    category_match_score=round(float(row["category_match_score"]), 4),
                    geography_match_score=round(float(row["geography_match_score"]), 4),
                    audience_fit_score=round(float(row["audience_fit_score"]), 4),
                    reason=reason,
                    appeared_in_events=row["appeared_in_events"],
                    appeared_count=int(row["frequency"]),
                )
            )

        # ── Insights ─────────────────────────────────────────────────
        insights = generate_insights(
            scored_df=scored_df,
            similar_events_df=similar,
            exhibitor_pool=exhibitor_pool,
            query_category=request.category,
            query_geography=request.geography,
            query_audience=request.audience_size,
            clusters=clusters_raw,
        )

        # ── Assemble response ─────────────────────────────────────────
        similar_events_out = [
            SimilarEvent(
                event_id=row["event_id"],
                event_name=row["event_name"],
                category=row["category"],
                location=row["location"],
                audience_size=int(row["audience_size"]),
                similarity_score=round(float(row["similarity_score"]), 4),
                year=int(row["year"]),
            )
            for _, row in similar.iterrows()
        ]

        # Flatten clusters for response model
        flat_clusters: Dict[str, ClusterInfo] = {}
        rb = clusters_raw.get("rule_based", clusters_raw)
        for label, info in rb.items():
            flat_clusters[label] = ClusterInfo(
                exhibitors=info["exhibitors"],
                count=info["count"],
                percentage=info["percentage"],
                top_exhibitor=info.get("top_exhibitor"),
            )

        elapsed = round(time.perf_counter() - t0, 3)
        logger.info(f"Agent completed in {elapsed}s. Returning {len(recommendations)} recommendations.")

        return RecommendationResponse(
            query=request,
            similar_events_used=similar_events_out,
            recommended_exhibitors=recommendations,
            clusters=flat_clusters,
            insights=insights,
            metadata={
                "elapsed_seconds": elapsed,
                "agent_version": self.VERSION,
                "similar_events_count": len(similar_event_ids),
                "candidate_pool_size": len(scored_df),
                "data_hash": self.loader.data_hash,
            },
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _empty_response(
        self,
        request: RecommendationRequest,
        similar: pd.DataFrame,
        events_df: pd.DataFrame,
    ) -> RecommendationResponse:
        similar_events_out = [
            SimilarEvent(
                event_id=row["event_id"],
                event_name=row["event_name"],
                category=row["category"],
                location=row["location"],
                audience_size=int(row["audience_size"]),
                similarity_score=round(float(row["similarity_score"]), 4),
                year=int(row["year"]),
            )
            for _, row in similar.iterrows()
        ]
        return RecommendationResponse(
            query=request,
            similar_events_used=similar_events_out,
            recommended_exhibitors=[],
            clusters={},
            insights=["No exhibitors found for the given criteria."],
            metadata={"agent_version": self.VERSION},
        )