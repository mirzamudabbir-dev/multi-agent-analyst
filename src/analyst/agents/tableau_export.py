"""Tableau Export Agent — exports cleaned data to a Tableau .hyper extract."""

from __future__ import annotations

from analyst.base_agent import BaseAgent
from analyst.models import Severity, AgentResult
from analyst.registry import register_agent
from analyst.state import AnalysisState


@register_agent("tableau_export")
class TableauExportAgent(BaseAgent):
    """Exports data to a Tableau .hyper extract file."""

    name = "tableau_export"
    description = "Tableau Hyper Extract generation"

    def validate_preconditions(self, state: AnalysisState) -> None:
        if state.active_df is None:
            raise ValueError("No data available to export.")

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        try:
            import pantab
        except ImportError:
            state.log(self.name, "pantab library not found. Skipping Tableau export.", Severity.WARNING)
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="pantab library not found"
            )

        df = state.active_df
        assert df is not None

        output_path = state.output_dir / f"{state.file_path.stem}.hyper"
        
        try:
            # pantab expects pandas or pyarrow. Polars converts to pandas natively.
            pandas_df = df.to_pandas()
            pantab.frame_to_hyper(pandas_df, str(output_path), table="Extract")
            state.log(self.name, f"Exported Hyper extract to {output_path}", Severity.INFO)
            return state, AgentResult(
                agent_name=self.name,
                status="success",
                reasoning=f"Exported Hyper extract to {output_path}",
                data_summary={"file_size": output_path.stat().st_size}
            )
        except Exception as e:
            state.log(self.name, f"Failed to export Hyper file: {e}", Severity.ERROR)
            return state, AgentResult(
                agent_name=self.name,
                status="error",
                reasoning=f"Failed to export: {e}"
            )
