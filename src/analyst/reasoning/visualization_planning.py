"""Visualization Planning Agent - Decides what charts to build without drawing them."""

from __future__ import annotations

import json
from pydantic import BaseModel

from analyst.reasoning.base_reasoning import BaseReasoningAgent
from analyst.models import AgentResult, Severity
from analyst.registry import register_agent
from analyst.state import AnalysisState


class ChartModel(BaseModel):
    type: str
    x: str
    y: str
    title: str


class VisSchema(BaseModel):
    charts: list[ChartModel]


@register_agent("visualization_planner")
class VisualizationPlanningAgent(BaseReasoningAgent):
    """Outputs structured JSON chart configurations based strictly on analysis logic."""

    name = "visualization_planner"
    description = "Reasoning: Chart Specifications"

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        context = {
            "columns": [{"name": p.name, "type": str(p.dtype)} for p in (state.profile or [])],
            "trends_identified": state.eda_results.get("reasoning_trends", []) if state.eda_results else []
        }

        prompt = f"""You are a Data Visualization Planner Agent.
Recommend exactly 3 to 4 useful charts to build.
Context: {json.dumps(context, default=str)}

Supported types: 
- 'histogram' (needs only 'x')
- 'bar' (needs 'x', and optionally 'y'. If 'y' is empty, it plots the frequency count of 'x')
- 'scatter' (needs BOTH 'x' and 'y')

Constraints:
- You MUST ensure the 'x' and 'y' column names EXACTLY MATCH the names in the profile.
- Return ONLY valid column names or empty string "" for y if not needed.
"""
        response = self.generate_structured_response(prompt, VisSchema)

        if not response:
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="Failed to contact LLM or parse schema."
            )

        if state.eda_results is None:
            state.eda_results = {}
        
        # Store for Python execution
        state.eda_results["chart_instructions"] = [c.model_dump() for c in response.charts]

        state.log(self.name, f"Planned {len(response.charts)} visualizations.", Severity.INFO)

        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning=f"Planned {len(response.charts)} charts for execution layer.",
            data_summary={"charts_planned": len(response.charts)}
        )
