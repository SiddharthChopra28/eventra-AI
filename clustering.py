"""
clustering.py - Groups exhibitors into semantic clusters.

Primary: Rule-based type mapping (fast, interpretable, no external deps)
Bonus:   KMeans on TF-IDF of exhibitor names (sklearn, optional)
"""
import logging
from typing import Dict, List, Any, Optional
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

CLUSTER_LABELS = ["Startups", "Enterprises", "Tools/Platforms", "Others"]

TYPE_TO_CLUSTER: Dict[str, str] = {
    "Startup": "Startups",
    "Enterprise": "Enterprises",
    "Tool": "Tools/Platforms",
    "Platform": "Tools/Platforms",
    "Others": "Others",
}


def rule_based_clustering(scored_df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Groups exhibitors by their labelled type.
    Returns a dict keyed by cluster name.
    """
    clusters: Dict[str, List[str]] = {label: [] for label in CLUSTER_LABELS}
    total = len(scored_df)

    for _, row in scored_df.iterrows():
        cluster = TYPE_TO_CLUSTER.get(row["exhibitor_type"], "Others")
        clusters[cluster].append(row["exhibitor_name"])

    result = {}
    for label, names in clusters.items():
        top = None
        if names:
            # pick the one with highest score
            subset = scored_df[scored_df["exhibitor_name"].isin(names)]
            if not subset.empty:
                top = subset.sort_values("score", ascending=False).iloc[0]["exhibitor_name"]
        result[label] = {
            "exhibitors": names,
            "count": len(names),
            "percentage": round(len(names) / total * 100, 1) if total > 0 else 0.0,
            "top_exhibitor": top,
        }

    logger.debug(f"Rule-based clusters: { {k: v['count'] for k, v in result.items()} }")
    return result


def ml_clustering(scored_df: pd.DataFrame, n_clusters: int = 4) -> Optional[Dict[str, Any]]:
    """
    Optional KMeans clustering on feature vectors.
    Falls back gracefully if sklearn is unavailable or not enough samples.
    """
    try:
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler
    except ImportError:
        logger.info("sklearn not available; skipping ML clustering.")
        return None

    feature_cols = [
        "frequency_score",
        "category_match_score",
        "geography_match_score",
        "audience_fit_score",
        "score",
    ]

    available = [c for c in feature_cols if c in scored_df.columns]
    if len(scored_df) < n_clusters or len(available) < 2:
        logger.info("Not enough samples/features for ML clustering.")
        return None

    X = scored_df[available].fillna(0).values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    km = KMeans(n_clusters=min(n_clusters, len(scored_df)), random_state=42, n_init=10)
    labels = km.fit_predict(X_scaled)

    scored_df = scored_df.copy()
    scored_df["ml_cluster"] = labels

    clusters: Dict[str, Dict[str, Any]] = {}
    for cid in sorted(scored_df["ml_cluster"].unique()):
        subset = scored_df[scored_df["ml_cluster"] == cid]
        top_row = subset.sort_values("score", ascending=False).iloc[0]
        # Derive a human label from dominant type in the cluster
        dominant_type = subset["exhibitor_type"].mode()[0]
        label = f"Cluster_{cid+1}_{dominant_type}"
        clusters[label] = {
            "exhibitors": subset["exhibitor_name"].tolist(),
            "count": len(subset),
            "percentage": round(len(subset) / len(scored_df) * 100, 1),
            "top_exhibitor": top_row["exhibitor_name"],
            "avg_score": round(subset["score"].mean(), 2),
        }

    logger.info(f"ML clustering produced {len(clusters)} clusters.")
    return clusters


def cluster_exhibitors(
    scored_df: pd.DataFrame,
    use_ml: bool = True,
) -> Dict[str, Any]:
    """
    Runs rule-based clustering (always) and optionally ML clustering.
    Returns merged result with both views if ML is available.
    """
    rule_clusters = rule_based_clustering(scored_df)

    if use_ml:
        ml_result = ml_clustering(scored_df)
        if ml_result:
            return {
                "rule_based": rule_clusters,
                "ml_based": ml_result,
            }

    return {"rule_based": rule_clusters}