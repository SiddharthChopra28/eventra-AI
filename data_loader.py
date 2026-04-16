"""
data_loader.py - Loads and preprocesses historical event data with caching
"""
import json
import logging
import hashlib
from pathlib import Path
from functools import lru_cache
from typing import List, Dict, Any, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Loads historical event data from JSON, validates it, and
    builds normalized DataFrames for downstream processing.
    """

    DEFAULT_DATA_PATH = Path(__file__).parent / "sample_data" / "events.json"

    def __init__(self, data_path: Optional[str] = None):
        self.data_path = Path(data_path) if data_path else self.DEFAULT_DATA_PATH
        self._raw_data: Optional[List[Dict]] = None
        self._events_df: Optional[pd.DataFrame] = None
        self._exhibitors_df: Optional[pd.DataFrame] = None
        self._data_hash: Optional[str] = None
        logger.info(f"DataLoader initialised with path: {self.data_path}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(self) -> "DataLoader":
        """Load and pre-process data. Returns self for chaining."""
        raw = self._read_json()
        self._raw_data = raw
        self._data_hash = self._compute_hash(raw)
        self._events_df = self._build_events_df(raw)
        self._exhibitors_df = self._build_exhibitors_df(raw)
        logger.info(
            f"Loaded {len(self._events_df)} events, "
            f"{len(self._exhibitors_df)} exhibitor-rows"
        )
        return self

    @property
    def events_df(self) -> pd.DataFrame:
        self._ensure_loaded()
        return self._events_df  # type: ignore

    @property
    def exhibitors_df(self) -> pd.DataFrame:
        self._ensure_loaded()
        return self._exhibitors_df  # type: ignore

    @property
    def data_hash(self) -> str:
        self._ensure_loaded()
        return self._data_hash  # type: ignore

    def summary(self) -> Dict[str, Any]:
        self._ensure_loaded()
        return {
            "total_events": len(self._events_df),
            "total_exhibitor_rows": len(self._exhibitors_df),
            "unique_exhibitors": self._exhibitors_df["exhibitor_name"].nunique(),
            "categories": self._events_df["category"].unique().tolist(),
            "countries": self._events_df["country"].unique().tolist(),
            "year_range": [
                int(self._events_df["year"].min()),
                int(self._events_df["year"].max()),
            ],
            "data_hash": self._data_hash,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _ensure_loaded(self):
        if self._events_df is None:
            raise RuntimeError("DataLoader.load() must be called before accessing data.")

    def _read_json(self) -> List[Dict]:
        if not self.data_path.exists():
            raise FileNotFoundError(f"Dataset not found at: {self.data_path}")
        with open(self.data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list) or len(data) == 0:
            raise ValueError("Dataset must be a non-empty JSON array of events.")
        logger.debug(f"Raw JSON loaded: {len(data)} records")
        return data

    @staticmethod
    def _compute_hash(data: List[Dict]) -> str:
        blob = json.dumps(data, sort_keys=True).encode()
        return hashlib.md5(blob).hexdigest()[:8]

    @staticmethod
    def _build_events_df(raw: List[Dict]) -> pd.DataFrame:
        rows = []
        for evt in raw:
            rows.append(
                {
                    "event_id": evt.get("event_id", ""),
                    "event_name": evt.get("event_name", ""),
                    "category": evt.get("category", "").strip(),
                    "subcategory": evt.get("subcategory", "").strip(),
                    "location": evt.get("location", ""),
                    "country": evt.get("country", "").strip(),
                    "audience_size": int(evt.get("audience_size", 0)),
                    "year": int(evt.get("year", 2023)),
                    "num_exhibitors": len(evt.get("exhibitors", [])),
                }
            )
        df = pd.DataFrame(rows)
        df["category_lower"] = df["category"].str.lower()
        df["country_lower"] = df["country"].str.lower()
        return df

    @staticmethod
    def _build_exhibitors_df(raw: List[Dict]) -> pd.DataFrame:
        rows = []
        for evt in raw:
            event_id = evt.get("event_id", "")
            category = evt.get("category", "")
            country = evt.get("country", "")
            audience_size = int(evt.get("audience_size", 0))
            for ex in evt.get("exhibitors", []):
                rows.append(
                    {
                        "event_id": event_id,
                        "event_category": category,
                        "event_country": country,
                        "event_audience_size": audience_size,
                        "exhibitor_name": ex.get("name", "").strip(),
                        "exhibitor_type": ex.get("type", "Others").strip(),
                        "booth_size": ex.get("booth_size", "small").strip(),
                    }
                )
        df = pd.DataFrame(rows)
        df["exhibitor_name_lower"] = df["exhibitor_name"].str.lower()
        df["event_category_lower"] = df["event_category"].str.lower()
        df["event_country_lower"] = df["event_country"].str.lower()
        return df