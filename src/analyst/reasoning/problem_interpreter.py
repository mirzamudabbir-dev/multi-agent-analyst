"""Problem Interpreter Agent - converts user query into an analytical plan."""

from __future__ import annotations

import json
from pydantic import BaseModel

from analyst.reasoning.base_reasoning import BaseReasoningAgent
from analyst.models import AgentResult, Severity
from analyst.registry import register_agent
from analyst.state import AnalysisState


class InterpretationSchema(BaseModel):
    metrics: list[str]
    dimensions: list[str]
    hypotheses: list[str]
    analytical_plan_steps: list[str]


@register_agent("problem_interpreter")
class ProblemInterpreterAgent(BaseReasoningAgent):
    """Converts a raw user query/goal into a structured analytical plan."""

    name = "problem_interpreter"
    description = "Reasoning: Interpret Goal"

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        goal = state.config.get("goal", "Analyze this dataset fully")
        
        prompt = f"""You are a top-tier Data Science Planner.
The user has provided a goal: '{goal}'
Please interpret this goal and break it down into a structured analytical plan.
Identify the key metrics, dimensions/slices to look at, and potential hypotheses to test.
"""
        response = self.generate_structured_response(prompt, InterpretationSchema)
        
        if not response:
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="Failed to contact LLM or parse schema."
            )

        # Save to state
        if not hasattr(state, "plan") or state.plan is None:
            state.plan = {}
            
        state.plan["interpreter_metrics"] = response.metrics
        state.plan["interpreter_dimensions"] = response.dimensions
        state.plan["interpreter_hypotheses"] = response.hypotheses
        state.plan["interpreter_steps"] = response.analytical_plan_steps

        state.log(self.name, f"Interpreted {len(response.analytical_plan_steps)} planning steps.", Severity.INFO)
        
        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning="Constructed formal analytical plan from user objective.",
            data_summary={"identified_metrics": len(response.metrics), "hypotheses": len(response.hypotheses)}
        )
