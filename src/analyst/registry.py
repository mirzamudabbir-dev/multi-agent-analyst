"""Agent registry for dynamic discovery and lookup."""

from __future__ import annotations

from typing import Type

from analyst.base_agent import BaseAgent

# Global registry: name → agent class
_REGISTRY: dict[str, Type[BaseAgent]] = {}


def register_agent(name: str):
    """
    Decorator to register an agent class by name.

    Usage:
        @register_agent("ingestion")
        class IngestionAgent(BaseAgent): ...
    """

    def decorator(cls: Type[BaseAgent]) -> Type[BaseAgent]:
        cls.name = name
        _REGISTRY[name] = cls
        return cls

    return decorator


def get_agent(name: str) -> BaseAgent:
    """Instantiate a registered agent by name."""
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise KeyError(f"Agent '{name}' not found. Available: {available}")
    return _REGISTRY[name]()


def get_all_agents() -> dict[str, Type[BaseAgent]]:
    """Return a copy of the full registry."""
    return dict(_REGISTRY)


def default_pipeline_order() -> list[str]:
    """The default ordered sequence of agents for a full analysis."""
    return [
        "ingestion",
        "profiling",
        "cleaning",
        "eda",
        "visualization",
        "reporting",
        "excel_export",
    ]

# Must import agents and reasoning modules to execute the @register_agent decorators
import analyst.agents.ingestion
import analyst.agents.profiling
import analyst.agents.cleaning
import analyst.agents.eda
import analyst.agents.visualization
import analyst.agents.reporting
import analyst.agents.tableau_export
import analyst.agents.excel_export
import analyst.agents.query_execution
import analyst.agents.transformation

import analyst.reasoning.consolidated
import analyst.reasoning.query_generation
import analyst.reasoning.data_discovery
import analyst.reasoning.problem_interpreter
import analyst.reasoning.insight_report
import analyst.reasoning.eda_reasoning
import analyst.reasoning.visualization_planning

