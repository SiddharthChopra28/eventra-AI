"""
similarity.py - Computes similarity between the query event and historical events.

Weights:
  category  -> 0.50  (highest — category mismatch is a hard signal)
  geography -> 0.30
  audience  -> 0.20
"""
import logging
from typing import List, Dict, Any, Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Category synonym map so "ML" and "Machine Learning" are treated alike
CATEGORY_SYNONYMS: Dict[str, List[str]] = {
    "ai": ["artificial intelligence", "machine learning", "ml", "deep learning",
           "nlp", "generative ai", "llm", "data science", "computer vision",
           "robotics", "research"],
    "data science": ["data analytics", "big data", "analytics", "data engineering"],
    "cloud": ["cloud computing", "saas", "paas", "infrastructure"],
    "fintech": ["finance", "banking", "payments", "insurtech"],
    "cybersecurity": ["security", "infosec", "devsecops"],
    "iot": ["internet of things", "edge computing", "embedded"],
}

GEOGRAPHY_SYNONYMS: Dict[str, List[str]] = {
    "india": ["bangalore", "mumbai", "delhi", "hyderabad", "pune", "chennai",
              "kolkata", "ahmedabad"],
    "usa": ["san francisco", "new york", "chicago", "seattle", "austin",
            "united states", "us"],
    "uae": ["dubai", "abu dhabi"],
    "singapore": ["sg"],
    "uk": ["london", "united kingdom", "england"],
}


def _expand_to_group(value: str, synonym_map: Dict[str, List[str]]) -> str:
    """Return the canonical group key that value belongs to, or value itself."""
    v = value.lower().strip()
    for canonical, variants in synonym_map.items():
        if v == canonical or v in variants:
            return canonical
    return v


def category_similarity(query_cat: str, event_cat: str, event_subcat: str = "") -> float:
    """
    Returns 0.0–1.0.
    1.0  -> same canonical group
    0.6  -> partial keyword overlap
    0.0  -> no relation
    """
    q = _expand_to_group(query_cat, CATEGORY_SYNONYMS)
    e = _expand_to_group(event_cat, CATEGORY_SYNONYMS)
    es = _expand_to_group(event_subcat, CATEGORY_SYNONYMS) if event_subcat else ""

    if q == e:
        return 1.0
    if q == es:
        return 0.8

    # Partial overlap via shared keywords
    q_tokens = set(q.replace("-", " ").split())
    e_tokens = set(e.replace("-", " ").split())
    if q_tokens & e_tokens:
        return 0.6

    return 0.0


def geography_similarity(query_geo: str, event_country: str, event_location: str = "") -> float:
    """
    Returns 0.0–1.0.
    1.0 -> same canonical country group
    0.5 -> partial match / same region
    0.0 -> different region
    """
    q = _expand_to_group(query_geo, GEOGRAPHY_SYNONYMS)
    ec = _expand_to_group(event_country, GEOGRAPHY_SYNONYMS)
    el = _expand_to_group(event_location, GEOGRAPHY_SYNONYMS) if event_location else ""

    if q == ec:
        return 1.0
    if q == el:
        return 0.9

    # Asia proximity bonus
    asia_group = {"india", "singapore", "uae", "japan", "china", "southeast asia"}
    if q in asia_group and ec in asia_group:
        return 0.4

    return 0.0


def audience_similarity(query_size: int, event_size: int) -> float:
    """
    Gaussian decay based on relative difference.
    Same size -> 1.0; 50% off -> ~0.6; 3x off -> ~0.1
    """
    if event_size == 0:
        return 0.5
    ratio = min(query_size, event_size) / max(query_size, event_size)
    # Smooth curve: 1.0 at ratio=1, approaching 0 as ratio→0
    return float(ratio ** 0.5)


def compute_event_similarities(
    query_category: str,
    query_geography: str,
    query_audience: int,
    events_df: pd.DataFrame,
    weights: Tuple[float, float, float] = (0.5, 0.3, 0.2),
) -> pd.DataFrame:
    """
    Returns events_df with an extra `similarity_score` column (0–1),
    sorted descending.
    """
    w_cat, w_geo, w_aud = weights

    df = events_df.copy()
    df["cat_sim"] = df.apply(
        lambda r: category_similarity(query_category, r["category"], r.get("subcategory", "")),
        axis=1,
    )
    df["geo_sim"] = df.apply(
        lambda r: geography_similarity(query_geography, r["country"], r.get("location", "")),
        axis=1,
    )
    df["aud_sim"] = df["audience_size"].apply(
        lambda s: audience_similarity(query_audience, s)
    )

    df["similarity_score"] = (
        w_cat * df["cat_sim"]
        + w_geo * df["geo_sim"]
        + w_aud * df["aud_sim"]
    )

    df.sort_values("similarity_score", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.debug(
        f"Similarity computed for {len(df)} events. "
        f"Top score: {df['similarity_score'].iloc[0]:.3f}"
    )
    return df