"""Profiling Agent — generates statistical profiles for every column."""

from __future__ import annotations

import polars as pl

from analyst.base_agent import BaseAgent
from analyst.models import ColumnProfile, Severity, AgentResult
from analyst.registry import register_agent
from analyst.state import AnalysisState


@register_agent("profiling")
class ProfilingAgent(BaseAgent):
    """Analyzes each column and populates state.profile."""

    name = "profiling"
    description = "Data profiling"

    def validate_preconditions(self, state: AnalysisState) -> None:
        if state.raw_df is None:
            raise ValueError("No data loaded. Run ingestion first.")

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        df = state.raw_df
        assert df is not None

        profiles: list[ColumnProfile] = []

        for col_name in df.columns:
            col = df[col_name]
            null_count = col.null_count()
            total = len(col)
            null_pct = (null_count / total * 100) if total > 0 else 0.0
            unique_count = col.n_unique()

            profile = ColumnProfile(
                name=col_name,
                dtype=str(col.dtype),
                null_count=null_count,
                null_pct=round(null_pct, 2),
                unique_count=unique_count,
            )

            if col.dtype.is_numeric():
                profile.mean = self._safe_stat(col, "mean")
                profile.median = self._safe_stat(col, "median")
                profile.std = self._safe_stat(col, "std")
                profile.min = self._safe_stat(col, "min")
                profile.max = self._safe_stat(col, "max")
            else:
                # Top categorical values
                profile.top_values = self._top_values(col, n=10)

            profiles.append(profile)

        state.profile = profiles
        state.memory_usage_bytes = df.estimated_size()

        n_numeric = sum(1 for p in profiles if p.mean is not None)
        n_categorical = len(profiles) - n_numeric
        state.log(
            self.name,
            f"Profiled {len(profiles)} columns ({n_numeric} numeric, {n_categorical} categorical)",
            Severity.INFO,
        )
        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning=f"Profiled {len(profiles)} columns ({n_numeric} numeric, {n_categorical} categorical)",
            data_summary={"numeric_cols": n_numeric, "categorical_cols": n_categorical}
        )

    @staticmethod
    def _safe_stat(col: pl.Series, stat: str) -> float | None:
        try:
            val = getattr(col, stat)()
            return round(float(val), 4) if val is not None else None
        except Exception:
            return None

    @staticmethod
    def _top_values(col: pl.Series, n: int = 10) -> list[tuple[str, int]]:
        try:
            counts = col.drop_nulls().value_counts(sort=True)
            # value_counts returns a DataFrame with columns named after the original and "count"
            col_name = counts.columns[0]
            result = []
            for row in counts.head(n).iter_rows():
                result.append((str(row[0]), int(row[1])))
            return result
        except Exception:
            return []
