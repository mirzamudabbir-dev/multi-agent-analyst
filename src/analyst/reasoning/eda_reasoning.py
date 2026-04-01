"""EDA Reasoning Agent - Interprets computed EDA statistics."""

from __future__ import annotations

import json
from pydantic import BaseModel

from analyst.reasoning.base_reasoning import BaseReasoningAgent
from analyst.models import AgentResult, Severity
from analyst.registry import register_agent
from analyst.state import AnalysisState


class EDASchema(BaseModel):
    trends: list[str]
    anomalies: list[str]
    correlations_identified: list[str]
    executive_summary: str


@register_agent("eda_reasoner")
class EDAReasoningAgent(BaseReasoningAgent):
    """Analyzes the raw statistical computations done by Python."""

    name = "eda_reasoner"
    description = "Reasoning: Interpret EDA stats"

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        if not state.eda_results:
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="No EDA results available to reason about. Run EDA Compute Agent first."
            )

        context = {
            "numeric_summary": state.eda_results.get("numeric_summary", {}),
            "categorical_summary": state.eda_results.get("categorical_summary", {}),
            "correlations": state.eda_results.get("correlation", {})
        }
        
        prompt = f"""You are an Exploratory Data Analysis Interpreter.
Review the following computed statistics. These were computed via Pandas/Polars.
Computed Stats: {json.dumps(context, default=str)}

Identify the core trends, any anomalies/outliers, and notable correlations.
"""
        response = self.generate_structured_response(prompt, EDASchema, api_key=state.api_key)
        
        if not response:
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="Failed to contact LLM or parse schema."
            )

        # Store interpretations back into state.eda_results
        state.eda_results["reasoning_trends"] = response.trends
        state.eda_results["reasoning_anomalies"] = response.anomalies
        state.eda_results["reasoning_correlations"] = response.correlations_identified
        state.eda_results["executive_summary"] = response.executive_summary

        state.log(self.name, "Reasoned over computed EDA statistics.", Severity.INFO)
        
        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning=f"Identified {len(response.trends)} trends and {len(response.anomalies)} anomalies.",
            data_summary={"trends": len(response.trends), "anomalies": len(response.anomalies)}
        )
