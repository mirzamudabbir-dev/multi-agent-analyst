"""Insight Report Agent - Generates the final human explanation."""

from __future__ import annotations

import json
from pydantic import BaseModel

from analyst.reasoning.base_reasoning import BaseReasoningAgent
from analyst.models import AgentResult, Severity
from analyst.registry import register_agent
from analyst.state import AnalysisState


class ReportSchema(BaseModel):
    markdown_report: str


@register_agent("insight_report_reasoner")
class InsightReportAgent(BaseReasoningAgent):
    """Converts the collected insights into a human-readable business explanation."""

    name = "insight_report_reasoner"
    description = "Reasoning: Generate Output Report"

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        context = {
            "goal": state.config.get("goal"),
            "trends": state.eda_results.get("reasoning_trends", []) if state.eda_results else [],
            "anomalies": state.eda_results.get("reasoning_anomalies", []) if state.eda_results else [],
            "cleaning_actions": [{"action": a.action, "column": a.column} for a in state.cleaning_actions]
        }

        prompt = f"""You are a Lead Data Analyst writing the final executive summary report.
Context gathered by previous agents:
{json.dumps(context, default=str)}

Generate an insightful, well-formatted Markdown report synthesizing these findings for a business stakeholder.
"""
        response = self.generate_structured_response(prompt, ReportSchema, api_key=state.api_key)

        if not response:
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="Failed to contact LLM or parse schema."
            )

        # Store back to state so an execution agent can save it
        if state.eda_results is None:
            state.eda_results = {}
        state.eda_results["final_markdown_report"] = response.markdown_report

        state.log(self.name, "Reasoned over full pipeline to generate business report.", Severity.INFO)

        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning="Constructed final markdown report.",
            data_summary={"report_length": len(response.markdown_report)}
        )
