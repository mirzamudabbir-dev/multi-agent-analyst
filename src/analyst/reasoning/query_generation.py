"""Query Generation Agent - Plans Pandas logic without executing it."""

from __future__ import annotations

import json
from pydantic import BaseModel

from analyst.reasoning.base_reasoning import BaseReasoningAgent
from analyst.models import AgentResult, Severity
from analyst.registry import register_agent
from analyst.state import AnalysisState


class QueryLogic(BaseModel):
    filter_conditions: list[str]
    groupby_columns: list[str]
    aggregations: dict[str, str]


class QueryGenerationSchema(BaseModel):
    operations: list[QueryLogic]
    reasoning: str


@register_agent("query_generation_reasoner")
class QueryGenerationAgent(BaseReasoningAgent):
    """Designs operations to answer the analytical plan without executing data."""

    name = "query_generation_reasoner"
    description = "Reasoning: Generate Queries"

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        context = {
            "columns": [{"name": p.name, "type": str(p.dtype)} for p in (state.profile or [])],
            "plan_metrics": state.plan.get("interpreter_metrics", []) if hasattr(state, "plan") else [],
            "plan_dimensions": state.plan.get("interpreter_dimensions", []) if hasattr(state, "plan") else [],
        }
        
        prompt = f"""You are a Query Generation Expert.
Based on the dataset schema and the dimensions/metrics to investigate, generate the logical query steps.
Dataset Profile: {json.dumps(context, default=str)}

Do not execute code. Merely output the logical 'filters', 'groupby_columns', and 'aggregations' mapped to columns.
Valid aggregations are 'mean', 'sum', 'count', 'max', 'min'.
"""
        response = self.generate_structured_response(prompt, QueryGenerationSchema)
        
        if not response:
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="Failed to contact LLM or parse schema."
            )

        if not hasattr(state, "plan") or state.plan is None:
            state.plan = {}
            
        state.plan["query_logic"] = [op.model_dump() for op in response.operations]

        state.log(self.name, "Generated abstract query logic.", Severity.INFO)
        
        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning=response.reasoning,
            data_summary={"operations_planned": len(response.operations)}
        )
