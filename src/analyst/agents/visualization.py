"""Visualization Execution Agent - purely generates matplotlib charts."""

from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import polars as pl

from analyst.base_agent import BaseAgent
from analyst.models import Severity, AgentResult
from analyst.registry import register_agent
from analyst.state import AnalysisState


@register_agent("visualization_execution")
class VisualizationExecutionAgent(BaseAgent):
    """Generates charts dynamically based on JSON instructions strictly without LLM calls."""

    name = "visualization_execution"
    description = "Execution: Matplotlib Chart Draw"

    def validate_preconditions(self, state: AnalysisState) -> None:
        if state.active_df is None:
            raise ValueError("No data available for visualization.")

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        df = state.active_df
        assert df is not None
        
        instructions = []
        if state.eda_results and "chart_instructions" in state.eda_results:
            instructions = state.eda_results["chart_instructions"]
            
        if not instructions:
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="No chart instructions planned by the LLM."
            )

        charts_dir = state.charts_dir or state.output_dir / "charts"
        charts_dir.mkdir(parents=True, exist_ok=True)

        try:
            plt.style.use("seaborn-v0_8-darkgrid")
        except OSError:
            plt.style.use("ggplot")

        generated: list[Path] = []

        # Generate Matplotlib Charts from Instructions
        for inst in instructions:
            ctype = inst.get("type", "")
            x_col = inst.get("x", "")
            y_col = inst.get("y", "")
            title = inst.get("title", f"{ctype} chart")

            if ctype == "histogram" and x_col in df.columns:
                path = self._histogram(df, x_col, title, charts_dir)
                if path: generated.append(path)
            elif ctype == "bar" and x_col in df.columns:
                path = self._bar(df, x_col, y_col, title, charts_dir)
                if path: generated.append(path)
            elif ctype == "scatter" and x_col in df.columns and y_col in df.columns:
                path = self._scatter(df, x_col, y_col, title, charts_dir)
                if path: generated.append(path)

        state.visualizations = generated
        state.log(self.name, f"Rendered {len(generated)} charts natively.", Severity.INFO)
        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning=f"Rendered {len(generated)} charts natively from planned specifications.",
            data_summary={"charts_count": len(generated)}
        )

    def _histogram(self, df: pl.DataFrame, x_col: str, title: str, charts_dir: Path) -> Path | None:
        try:
            data = df[x_col].drop_nulls().to_list()
            if not data: return None
            fig, ax = plt.subplots(figsize=(8, 5))
            ax.hist(data, bins=30, edgecolor="white", alpha=0.8)
            ax.set_title(title, fontweight="bold")
            ax.set_xlabel(x_col)
            ax.set_ylabel("Frequency")
            path = charts_dir / f"hist_{x_col}.png"
            fig.savefig(path, dpi=120, bbox_inches="tight")
            plt.close(fig)
            return path
        except BaseException:
            return None

    def _bar(self, df: pl.DataFrame, x_col: str, y_col: str, title: str, charts_dir: Path) -> Path | None:
        try:
            fig, ax = plt.subplots(figsize=(8, 5))
            if y_col and y_col in df.columns:
                agg = df.group_by(x_col).agg(pl.col(y_col).mean()).drop_nulls().sort(y_col, descending=True).head(15)
                labels = [str(x) for x in agg[x_col].to_list()]
                values = agg[y_col].to_list()
            else:
                vc = df[x_col].drop_nulls().value_counts(sort=True).head(15)
                labels = [str(x) for x in vc[x_col].to_list()]
                values = vc["count"].to_list()
                
            if not labels: return None
            ax.bar(labels, values, alpha=0.8)
            ax.set_title(title, fontweight="bold")
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col if y_col else "Count")
            plt.xticks(rotation=45, ha="right")
            path = charts_dir / f"bar_{x_col}.png"
            fig.savefig(path, dpi=120, bbox_inches="tight")
            plt.close(fig)
            return path
        except BaseException:
            return None

    def _scatter(self, df: pl.DataFrame, x_col: str, y_col: str, title: str, charts_dir: Path) -> Path | None:
        try:
            d = df.select([x_col, y_col]).drop_nulls()
            if len(d) == 0: return None
            if len(d) > 5000: d = d.sample(5000)
            
            fig, ax = plt.subplots(figsize=(8, 5))
            ax.scatter(d[x_col].to_list(), d[y_col].to_list(), alpha=0.5)
            ax.set_title(title, fontweight="bold")
            ax.set_xlabel(x_col)
            ax.set_ylabel(y_col)
            path = charts_dir / f"scatter_{x_col}_{y_col}.png"
            fig.savefig(path, dpi=120, bbox_inches="tight")
            plt.close(fig)
            return path
        except BaseException:
            return None
