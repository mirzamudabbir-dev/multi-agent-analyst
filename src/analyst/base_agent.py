"""Abstract base class for all agents."""

from __future__ import annotations

from abc import ABC, abstractmethod

from analyst.models import Severity, AgentResult
from analyst.state import AnalysisState


class BaseAgent(ABC):
    """
    Base class every specialist agent must extend.

    Agents process AnalysisState and return a structured result:
      state_out, result = agent.run(state_in)
    """

    name: str = "base"
    description: str = "Base agent"

    @abstractmethod
    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        """Execute the agent's logic and return updated state and structured result."""
        ...

    def validate_preconditions(self, state: AnalysisState) -> None:
        """
        Check that the state has everything this agent needs.
        Raise ValueError if preconditions are not met.
        Override in subclasses.
        """
        pass

    def execute(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        """
        Full lifecycle: validate → run → log success.
        Called by the orchestrator.
        """
        try:
            self.validate_preconditions(state)
            state.log(self.name, f"Starting {self.description}...")
            state, result = self.run(state)
            state.log(self.name, f"Completed successfully.", Severity.SUCCESS)
            return state, result
        except BaseException as e:
            state.log(self.name, f"Error: {e}", Severity.ERROR)
            return state, AgentResult(
                agent_name=self.name,
                status="error",
                reasoning=str(e),
            )
