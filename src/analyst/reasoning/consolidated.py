"""Consolidated Reasoning Agents to minimize LLM API calls on restricted quotas."""

from __future__ import annotations

import json
from pydantic import BaseModel

from analyst.reasoning.base_reasoning import BaseReasoningAgent
from analyst.models import AgentResult, Severity
from analyst.registry import register_agent
from analyst.state import AnalysisState


class StrategicPlanSchema(BaseModel):
    metrics: list[str]
    dimensions: list[str]
    hypotheses: list[str]
    analytical_plan_steps: list[str]
    primary_entities: list[str]
    schema_interpretation: str


@register_agent("strategic_planner")
class StrategicPlannerAgent(BaseReasoningAgent):
    """Reasoning Layer: Goal Interpretation + Data Discovery (Merged)."""
    name = "strategic_planner"
    description = "Reasoning: Strategic Plan & Discovery"

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        goal = state.config.get("goal", "Analyze this dataset fully")

        prompt = f"""You are a Strategic Data Scientist.
Goal: '{goal}'

{state.digest}

1. Interpret the user's intent into metrics/dimensions.
2. Formulate a step-by-step plan.
3. Identify what the primary entities represent.
"""
        response = self.generate_structured_response(prompt, StrategicPlanSchema, api_key=state.api_key)
        if not response:
            return state, AgentResult(agent_name=self.name, status="error", reasoning="LLM quota/error.")

        if not hasattr(state, "plan") or state.plan is None: state.plan = {}
        state.plan.update(response.model_dump())
        state.log(self.name, f"Strategic plan created: {response.schema_interpretation[:50]}...", Severity.INFO)

        return state, AgentResult(agent_name=self.name, status="success", reasoning="Consolidated plan & discovery complete.")


class QueryLogic(BaseModel):
    filter_conditions: list[str]
    groupby_columns: list[str]
    aggregations: dict[str, str]

class DataReasoningSchema(BaseModel):
    operations: list[QueryLogic]
    trends: list[str]
    anomalies: list[str]
    executive_summary: str


@register_agent("data_reasoner")
class DataReasoningAgent(BaseReasoningAgent):
    """Reasoning Layer: Query Generation + EDA Interpretation (Merged)."""
    name = "data_reasoner"
    description = "Reasoning: Query Logic & EDA Insights"

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        plan_metrics = state.plan.get("metrics", []) if hasattr(state, "plan") and state.plan else []

        prompt = f"""You are a Senior Data Analyst.
Review the dataset digest and plan metrics below.

{state.digest}

Plan Metrics: {plan_metrics}

1. Design the logical query operations (filters/groupbys) to answer the plan.
2. Interpret the computed statistics into trends and anomalies.
"""
        response = self.generate_structured_response(prompt, DataReasoningSchema, api_key=state.api_key)
        if not response:
            return state, AgentResult(agent_name=self.name, status="error", reasoning="LLM quota/error.")

        if not hasattr(state, "plan") or state.plan is None: state.plan = {}
        state.plan["query_logic"] = [op.model_dump() for op in response.operations]

        if state.eda_results is None: state.eda_results = {}
        state.eda_results["reasoning_trends"] = response.trends
        state.eda_results["reasoning_anomalies"] = response.anomalies
        state.eda_results["executive_summary"] = response.executive_summary

        return state, AgentResult(agent_name=self.name, status="success", reasoning="Generated query logic and interpreted EDA stats.")


class ChartModel(BaseModel):
    type: str
    x: str
    y: str
    title: str

class SynthesisSchema(BaseModel):
    charts: list[ChartModel]
    markdown_report: str


@register_agent("synthesis_expert")
class SynthesisAgent(BaseReasoningAgent):
    """Reasoning Layer: Visualization Planning + Final Reporting (Merged)."""
    name = "synthesis_expert"
    description = "Reasoning: Visualization & Synthesis"

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        trends = state.eda_results.get("reasoning_trends", []) if state.eda_results else []
        anomalies = state.eda_results.get("reasoning_anomalies", []) if state.eda_results else []

        prompt = f"""You are a Lead Data Storyteller.
Based on the found trends and anomalies, plan exactly 3-4 charts and write the final comprehensive report.

{state.digest}

Trends: {trends}
Anomalies: {anomalies}
"""
        response = self.generate_structured_response(prompt, SynthesisSchema, api_key=state.api_key)
        if not response:
            return state, AgentResult(agent_name=self.name, status="error", reasoning="LLM quota/error.")

        if state.eda_results is None: state.eda_results = {}
        state.eda_results["chart_instructions"] = [c.model_dump() for c in response.charts]
        state.eda_results["final_markdown_report"] = response.markdown_report

        return state, AgentResult(agent_name=self.name, status="success", reasoning="Planned visualizations and generated final report.")

