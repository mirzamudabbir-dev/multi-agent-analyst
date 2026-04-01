"""Data Discovery Reasoning Agent - Interprets dataset metadata."""

from __future__ import annotations

import json
from pydantic import BaseModel

from analyst.reasoning.base_reasoning import BaseReasoningAgent
from analyst.models import AgentResult, Severity
from analyst.registry import register_agent
from analyst.state import AnalysisState


class DiscoverySchema(BaseModel):
    primary_entities: list[str]
    potential_data_quality_issues: list[str]
    schema_interpretation: str


@register_agent("data_discovery_reasoner")
class DataDiscoveryReasonAgent(BaseReasoningAgent):
    """Analyzes schema and meta-data to discover analytical potential."""

    name = "data_discovery_reasoner"
    description = "Reasoning: Data Discovery"

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        if not state.profile:
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="No dataset profile available. Run ProfilingAgent first."
            )

        context = {
            "columns": state.col_count,
            "rows": state.row_count,
            "profile": [{"name": p.name, "type": str(p.dtype), "nulls": p.null_count, "unique": p.unique_count} for p in (state.profile or [])],
            "plan_metrics": state.plan.get("interpreter_metrics", []) if hasattr(state, "plan") else []
        }
        
        prompt = f"""You are a Data Discovery Expert.
Review the following dataset schema and the planned metrics.
Dataset Profile: {json.dumps(context, default=str)}

Identify what the primary entities represents (e.g. Sales, Users, Logs).
Note any glaring data quality warnings purely based on null counts or extreme uniqueness vs types.
"""
        response = self.generate_structured_response(prompt, DiscoverySchema, api_key=state.api_key)
        
        if not response:
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="Failed to contact LLM or parse schema."
            )

        if not hasattr(state, "plan") or state.plan is None:
            state.plan = {}
            
        state.plan["discovery_entities"] = response.primary_entities
        state.plan["discovery_quality_warnings"] = response.potential_data_quality_issues
        state.plan["discovery_interpretation"] = response.schema_interpretation

        state.log(self.name, "Completed data discovery reasoning on schema.", Severity.INFO)
        
        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning=f"Interpreted schema representing: {response.schema_interpretation[:50]}...",
            data_summary={"entities_found": len(response.primary_entities)}
        )
