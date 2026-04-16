"""
scoring.py - Computes relevance scores for each exhibitor candidate.

Formula (per spec):
  Relevance Score = 0.4 × Frequency Score
                  + 0.3 × Category Match Score
                  + 0.2 × Geography Match Score
                  + 0.1 × Audience Fit Score

All sub-scores are 0–1 before the final normalisation to 0–100.
"""
import logging
from typing import Dict, List, Any, Tuple
import pandas as pd
import numpy as np

from similarity import category_similarity, geography_similarity, audience_similarity

logger = logging.getLogger(__name__)

WEIGHTS = {
    "frequency": 0.40,
    "category": 0.30,
    "geography": 0.20,
    "audience": 0.10,
}


def _frequency_score(count: int, max_count: int) -> float:
    """Log-normalised frequency so a single dominant exhibitor doesn't drown others."""
    if max_count <= 0:
        return 0.0
    return float(np.log1p(count) / np.log1p(max_count))


def _category_match_score(
    exhibitor_name: str,
    exhibitor_events: pd.DataFrame,
    query_category: str,
) -> float:
    """
    Fraction of the exhibitor's appearances that are in the query category group.
    """
    total = len(exhibitor_events)
    if total == 0:
        return 0.0
    matched = exhibitor_events.apply(
        lambda r: category_similarity(query_category, r["event_category"]) > 0.5,
        axis=1,
    ).sum()
    return float(matched / total)


def _geography_match_score(
    exhibitor_events: pd.DataFrame,
    query_geography: str,
) -> float:
    """Fraction of appearances that are in the query geography."""
    total = len(exhibitor_events)
    if total == 0:
        return 0.0
    matched = exhibitor_events.apply(
        lambda r: geography_similarity(query_geography, r["event_country"]) > 0.5,
        axis=1,
    ).sum()
    return float(matched / total)


def _audience_fit_score(
    exhibitor_events: pd.DataFrame,
    query_audience: int,
) -> float:
    """Average audience similarity across all exhibitor appearances."""
    if exhibitor_events.empty:
        return 0.5
    sims = exhibitor_events["event_audience_size"].apply(
        lambda s: audience_similarity(query_audience, s)
    )
    return float(sims.mean())


def score_exhibitors(
    exhibitor_pool: pd.DataFrame,      # rows from exhibitors_df matching similar events
    all_exhibitor_rows: pd.DataFrame,  # full exhibitors_df (for global stats)
    query_category: str,
    query_geography: str,
    query_audience: int,
    similar_event_ids: List[str],
) -> pd.DataFrame:
    """
    Computes per-exhibitor relevance score.

    Returns a DataFrame with columns:
        exhibitor_name, exhibitor_type, frequency, frequency_score,
        category_match_score, geography_match_score, audience_fit_score,
        raw_score, score (0–100), appeared_in_events
    """
    # --- aggregate frequency within similar events ---
    freq_df = (
        exhibitor_pool.groupby("exhibitor_name")
        .agg(
            frequency=("event_id", "count"),
            exhibitor_type=("exhibitor_type", lambda x: x.mode()[0]),
            appeared_in_events=("event_id", lambda x: list(x.unique())),
        )
        .reset_index()
    )

    if freq_df.empty:
        logger.warning("No exhibitors found in similar events.")
        return pd.DataFrame()

    max_freq = freq_df["frequency"].max()

    records = []
    for _, row in freq_df.iterrows():
        name = row["exhibitor_name"]
        # All historical appearances (not just similar events) for signal breadth
        ex_rows = all_exhibitor_rows[all_exhibitor_rows["exhibitor_name"] == name]

        fs = _frequency_score(row["frequency"], max_freq)
        cs = _category_match_score(name, ex_rows, query_category)
        gs = _geography_match_score(ex_rows, query_geography)
        aus = _audience_fit_score(ex_rows, query_audience)

        raw = (
            WEIGHTS["frequency"] * fs
            + WEIGHTS["category"] * cs
            + WEIGHTS["geography"] * gs
            + WEIGHTS["audience"] * aus
        )

        records.append(
            {
                "exhibitor_name": name,
                "exhibitor_type": row["exhibitor_type"],
                "frequency": int(row["frequency"]),
                "appeared_in_events": row["appeared_in_events"],
                "frequency_score": round(fs, 4),
                "category_match_score": round(cs, 4),
                "geography_match_score": round(gs, 4),
                "audience_fit_score": round(aus, 4),
                "raw_score": round(raw, 4),
            }
        )

    scored_df = pd.DataFrame(records)

    # --- normalise raw_score to 0–100 ---
    min_raw = scored_df["raw_score"].min()
    max_raw = scored_df["raw_score"].max()
    if max_raw > min_raw:
        scored_df["score"] = (
            (scored_df["raw_score"] - min_raw) / (max_raw - min_raw) * 100
        ).round(2)
    else:
        scored_df["score"] = 100.0

    scored_df.sort_values("score", ascending=False, inplace=True)
    scored_df.reset_index(drop=True, inplace=True)

    logger.info(f"Scored {len(scored_df)} exhibitors. Top: {scored_df.iloc[0]['exhibitor_name']} ({scored_df.iloc[0]['score']})")
    return scored_df


def build_reason(row: pd.Series, events_df: pd.DataFrame, query_geography: str) -> str:
    """
    Constructs a human-readable, data-backed reason string for a recommendation.
    """
    parts = []

    # Frequency signal
    count = row["frequency"]
    event_names = []
    for eid in row["appeared_in_events"]:
        match = events_df[events_df["event_id"] == eid]["event_name"]
        if not match.empty:
            event_names.append(match.iloc[0])
    event_sample = ", ".join(event_names[:3])
    if count == 1:
        parts.append(f"Appeared in 1 similar event ({event_sample})")
    else:
        parts.append(f"Appeared in {count} similar events (e.g., {event_sample})")

    # Category signal
    cs = row["category_match_score"]
    if cs >= 0.8:
        parts.append("consistently participates in this category")
    elif cs >= 0.5:
        parts.append("has strong category alignment")
    elif cs > 0:
        parts.append("has partial category relevance")

    # Geography signal
    gs = row["geography_match_score"]
    if gs >= 0.8:
        parts.append(f"strong presence in {query_geography}")
    elif gs >= 0.4:
        parts.append(f"regional proximity to {query_geography}")

    # Type note
    etype = row["exhibitor_type"]
    if etype == "Startup":
        parts.append("emerging player in the space")
    elif etype == "Enterprise":
        parts.append("established enterprise exhibitor")
    elif etype in ("Tool", "Platform"):
        parts.append(f"popular {etype.lower()} used across similar events")

    return "; ".join(parts).capitalize() + "."