"""
insights.py - Derives human-readable insights from pattern analysis.
"""
import logging
from typing import List, Dict, Any
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def generate_insights(
    scored_df: pd.DataFrame,
    similar_events_df: pd.DataFrame,
    exhibitor_pool: pd.DataFrame,
    query_category: str,
    query_geography: str,
    query_audience: int,
    clusters: Dict[str, Any],
) -> List[str]:
    """
    Analyses patterns and returns a list of data-backed insight strings.
    """
    insights: List[str] = []

    if scored_df.empty:
        return ["No exhibitors found for the given criteria. Try broadening category or geography."]

    # --- 1. Type distribution in the pool ---
    type_dist = scored_df["exhibitor_type"].value_counts(normalize=True) * 100
    dominant_type = type_dist.idxmax()
    dominant_pct = round(type_dist.max(), 1)
    insights.append(
        f"{dominant_pct}% of exhibitors in similar {query_category} events "
        f"in {query_geography} are {dominant_type}s."
    )

    # --- 2. Top repeat exhibitors ---
    top_repeat = scored_df[scored_df["frequency"] >= 2].head(5)
    if not top_repeat.empty:
        names = ", ".join(top_repeat["exhibitor_name"].tolist())
        insights.append(
            f"Top repeat exhibitors across similar events: {names}."
        )

    # --- 3. Category coverage ---
    total_events = len(similar_events_df)
    cat_events = similar_events_df[
        similar_events_df["category"].str.lower() == query_category.lower()
    ]
    if total_events > 0:
        cat_pct = round(len(cat_events) / total_events * 100, 1)
        insights.append(
            f"{cat_pct}% of the {total_events} similar events used are "
            f"directly in the '{query_category}' category."
        )

    # --- 4. Geography concentration ---
    geo_events = similar_events_df[
        similar_events_df["country"].str.lower() == query_geography.lower()
    ]
    if not geo_events.empty:
        insights.append(
            f"{len(geo_events)} of {total_events} similar events took place in "
            f"{query_geography} — strong local signal available."
        )
    else:
        insights.append(
            f"No events found directly in {query_geography}; "
            f"recommendations are based on category and regional proximity."
        )

    # --- 5. Audience size trend ---
    if not similar_events_df.empty:
        avg_aud = int(similar_events_df["audience_size"].mean())
        diff = abs(query_audience - avg_aud)
        pct_diff = round(diff / avg_aud * 100, 1) if avg_aud > 0 else 0
        if pct_diff <= 20:
            insights.append(
                f"Your target audience ({query_audience:,}) is well-aligned with "
                f"similar events (avg {avg_aud:,}). Exhibitor fit is high."
            )
        elif query_audience > avg_aud:
            insights.append(
                f"Your event ({query_audience:,} attendees) is larger than typical "
                f"similar events (avg {avg_aud:,}). Consider enterprise exhibitors "
                f"for broader brand reach."
            )
        else:
            insights.append(
                f"Your event ({query_audience:,} attendees) is smaller than typical "
                f"similar events (avg {avg_aud:,}). Startups and niche tool vendors "
                f"may be better fits."
            )

    # --- 6. Tools/Platform trend ---
    rb = clusters.get("rule_based", clusters)
    tool_cluster = rb.get("Tools/Platforms", {})
    tool_pct = tool_cluster.get("percentage", 0)
    if tool_pct >= 30:
        insights.append(
            f"Tools and Platforms make up {tool_pct}% of recommendations — "
            f"this category drives significant tooling adoption at events."
        )

    # --- 7. Enterprise vs Startup balance ---
    ent_cluster = rb.get("Enterprises", {})
    start_cluster = rb.get("Startups", {})
    ent_count = ent_cluster.get("count", 0)
    start_count = start_cluster.get("count", 0)
    if ent_count > 0 and start_count > 0:
        ratio = round(ent_count / start_count, 1)
        insights.append(
            f"Enterprise-to-Startup ratio: {ratio}:1 — "
            f"{'Enterprise-heavy' if ratio > 2 else 'balanced' if ratio > 0.8 else 'Startup-heavy'} "
            f"exhibitor mix for this event profile."
        )

    # --- 8. High-frequency flag ---
    top1 = scored_df.iloc[0]
    if top1["frequency"] >= 3:
        insights.append(
            f"'{top1['exhibitor_name']}' is the most consistent exhibitor, "
            f"appearing in {top1['frequency']} similar events — a reliable anchor exhibitor."
        )

    logger.debug(f"Generated {len(insights)} insights.")
    return insights