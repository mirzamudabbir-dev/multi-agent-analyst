"""Transformation Execution Agent - Executes heavy grouping/aggregation logic native in Polars."""

from __future__ import annotations

import polars as pl

from analyst.base_agent import BaseAgent
from analyst.models import AgentResult, Severity
from analyst.registry import register_agent
from analyst.state import AnalysisState


@register_agent("transformation")
class TransformationAgent(BaseAgent):
    """Execution Layer: Performs pure Python Feature Creation and Groupbys."""

    name = "transformation"
    description = "Execution: Data Transformation"

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        df = state.active_df
        if df is None:
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="No Active DataFrame available."
            )

        instructions = []
        if hasattr(state, "plan") and state.plan:
            instructions = state.plan.get("query_logic", [])
            
        if not instructions:
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="No transformation operations logically planned."
            )

        applied = 0
        for logic in instructions:
            groupbys = logic.get("groupby_columns", [])
            aggs = logic.get("aggregations", {})
            
            if groupbys and aggs:
                try:
                    exprs = []
                    for col, op in aggs.items():
                        if col in df.columns:
                            if op == "mean": exprs.append(pl.col(col).mean().alias(f"{col}_mean"))
                            elif op == "sum": exprs.append(pl.col(col).sum().alias(f"{col}_sum"))
                            elif op == "count": exprs.append(pl.count().alias(f"{col}_count"))
                    
                    if exprs:
                        # Perform extreme heavy lifting
                        agged = df.group_by(groupbys).agg(exprs)
                        applied += 1
                except Exception as e:
                    state.log(self.name, f"Failed computation: {e}", Severity.WARNING)

        state.log(self.name, f"Successfully executed native Polars aggregations.", Severity.INFO)
        
        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning=f"Completed {applied} native python aggregation transformations.",
            data_summary={"aggregations_completed": applied}
        )
