"""EDA Agent — computes distributions, correlations, and outliers."""

from __future__ import annotations

import polars as pl

from analyst.base_agent import BaseAgent
from analyst.models import Severity, AgentResult
from analyst.registry import register_agent
from analyst.state import AnalysisState


@register_agent("eda")
class EDAAgent(BaseAgent):
    """Performs exploratory data analysis and populates state.eda_results."""

    name = "eda"
    description = "Exploratory data analysis"

    def validate_preconditions(self, state: AnalysisState) -> None:
        if state.active_df is None:
            raise ValueError("No data available. Run ingestion/cleaning first.")

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        df = state.active_df
        assert df is not None
        cfg = state.config.get("eda", {})

        results: dict = {}

        # 1. Numeric distributions
        numeric_cols = [c for c in df.columns if df[c].dtype.is_numeric()]
        categorical_cols = [c for c in df.columns if not df[c].dtype.is_numeric()]

        results["numeric_summary"] = self._numeric_summary(df, numeric_cols)
        results["categorical_summary"] = self._categorical_summary(df, categorical_cols)

        # 2. Correlation matrix
        if len(numeric_cols) >= 2:
            results["correlation"] = self._correlation_matrix(df, numeric_cols)

        # 3. Outlier detection
        outlier_method = cfg.get("outlier_method", "iqr")
        threshold = cfg.get("outlier_threshold", 1.5)
        results["outliers"] = self._detect_outliers(
            df, numeric_cols, method=outlier_method, threshold=threshold
        )

        state.eda_results = results

        n_outlier_cols = sum(1 for v in results.get("outliers", {}).values() if v["count"] > 0)
        state.log(
            self.name,
            f"Analyzed {len(numeric_cols)} numeric + {len(categorical_cols)} categorical columns. "
            f"Outliers found in {n_outlier_cols} columns.",
            Severity.INFO,
        )
        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning=f"Computed EDA. Outliers found in {n_outlier_cols} columns.",
            data_summary={"outlier_columns_count": n_outlier_cols}
        )

    @staticmethod
    def _numeric_summary(df: pl.DataFrame, cols: list[str]) -> dict:
        summary = {}
        for col_name in cols:
            col = df[col_name].drop_nulls()
            if len(col) == 0:
                continue

            try:
                q25 = col.quantile(0.25)
                q50 = col.quantile(0.50)
                q75 = col.quantile(0.75)
            except Exception:
                q25 = q50 = q75 = None

            summary[col_name] = {
                "mean": _round(col.mean()),
                "median": _round(col.median()),
                "std": _round(col.std()),
                "min": _round(col.min()),
                "max": _round(col.max()),
                "q25": _round(q25),
                "q50": _round(q50),
                "q75": _round(q75),
                "skew": _round(_safe_skew(col)),
            }
        return summary

    @staticmethod
    def _categorical_summary(df: pl.DataFrame, cols: list[str]) -> dict:
        summary = {}
        for col_name in cols:
            col = df[col_name].drop_nulls()
            unique = col.n_unique()

            # Top 10 values
            try:
                vc = col.value_counts(sort=True).head(10)
                top = [(str(row[0]), int(row[1])) for row in vc.iter_rows()]
            except Exception:
                top = []

            summary[col_name] = {
                "unique_count": unique,
                "top_values": top,
            }
        return summary

    @staticmethod
    def _correlation_matrix(df: pl.DataFrame, cols: list[str]) -> dict:
        """Compute pairwise Pearson correlation for numeric columns."""
        # Use Polars DataFrame.corr() for efficient computation
        corr_df = df.select(cols).corr()
        corr = {}
        for i, col_a in enumerate(cols):
            row = {}
            for j, col_b in enumerate(cols):
                val = corr_df[i, j]
                row[col_b] = _round(val)
            corr[col_a] = row
        return corr

    @staticmethod
    def _detect_outliers(
        df: pl.DataFrame,
        cols: list[str],
        method: str = "iqr",
        threshold: float = 1.5,
    ) -> dict:
        outliers = {}
        for col_name in cols:
            col = df[col_name].drop_nulls()
            if len(col) < 4:
                outliers[col_name] = {"count": 0, "pct": 0.0, "bounds": None}
                continue

            if method == "iqr":
                q1 = col.quantile(0.25)
                q3 = col.quantile(0.75)
                if q1 is None or q3 is None:
                    outliers[col_name] = {"count": 0, "pct": 0.0, "bounds": None}
                    continue
                iqr = q3 - q1
                lower = q1 - threshold * iqr
                upper = q3 + threshold * iqr
                mask = (col < lower) | (col > upper)
                count = mask.sum()
            else:  # zscore
                mean = col.mean()
                std = col.std()
                if std is None or std == 0:
                    outliers[col_name] = {"count": 0, "pct": 0.0, "bounds": None}
                    continue
                z = ((col - mean) / std).abs()
                lower = mean - threshold * std
                upper = mean + threshold * std
                count = (z > threshold).sum()

            pct = round(count / len(col) * 100, 2) if len(col) > 0 else 0
            outliers[col_name] = {
                "count": int(count),
                "pct": pct,
                "bounds": {"lower": _round(lower), "upper": _round(upper)},
            }
        return outliers


def _round(val, decimals: int = 4) -> float | None:
    if val is None:
        return None
    try:
        f = float(val)
        if f != f:  # NaN check
            return None
        return round(f, decimals)
    except (TypeError, ValueError):
        return None


def _safe_skew(col: pl.Series) -> float | None:
    """Compute skewness manually: E[(x-μ)³] / σ³."""
    try:
        mean = col.mean()
        std = col.std()
        if std is None or std == 0 or mean is None:
            return None
        n = len(col)
        skew = ((col - mean) ** 3).mean() / (std ** 3)
        return float(skew) if skew is not None else None
    except Exception:
        return None
