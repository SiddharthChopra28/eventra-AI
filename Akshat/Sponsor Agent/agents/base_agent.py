"""
Base Agent — abstract interface that every agent must implement.
Ensures modularity: new agents just subclass BaseAgent.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseAgent(ABC):
    """Abstract base class for all agents in the system."""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def run(self, event_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the agent's task.

        Args:
            event_context: Dictionary containing:
                - category: str (e.g., "AI", "Web3", "Music Festival")
                - geography: str (e.g., "India", "USA", "Europe")
                - target_audience_size: int
                - theme_keywords: list[str] (optional)
                - budget_range: dict with "min" and "max" (optional)

        Returns:
            Dictionary with:
                - agent_name: str
                - status: "completed" | "error"
                - results: dict (agent-specific)
                - context_updates: dict (shared context for other agents)
        """
        pass

    def _build_response(
        self,
        status: str,
        results: Dict[str, Any],
        context_updates: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        return {
            "agent_name": self.name,
            "status": status,
            "results": results,
            "context_updates": context_updates or {},
        }
