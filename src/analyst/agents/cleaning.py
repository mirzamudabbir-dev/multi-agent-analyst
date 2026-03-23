"""Cleaning Agent — handles duplicates, missing values, type coercion, column normalization."""

from __future__ import annotations

import re

import polars as pl

from analyst.base_agent import BaseAgent
from analyst.models import CleaningAction, Severity, AgentResult
from analyst.registry import register_agent
from analyst.state import AnalysisState


@register_agent("cleaning")
class CleaningAgent(BaseAgent):
    """Cleans the dataset and populates state.cleaned_df."""

    name = "cleaning"
    description = "Data cleaning"

    def validate_preconditions(self, state: AnalysisState) -> None:
        if state.raw_df is None:
            raise ValueError("No data loaded. Run ingestion first.")

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        df = state.raw_df.clone()
        assert df is not None
        cfg = state.config.get("cleaning", {})
        actions: list[CleaningAction] = []

        # 1. Column name normalization
        if cfg.get("normalize_columns", True):
            df, action = self._normalize_columns(df)
            if action:
                actions.append(action)

        # 2. Drop duplicates
        if cfg.get("drop_duplicates", True):
            df, action = self._drop_duplicates(df)
            if action:
                actions.append(action)

        # 3. Handle missing values
        df, missing_actions = self._handle_missing(df, cfg)
        actions.extend(missing_actions)

        # 4. Type coercion
        df, coercion_actions = self._coerce_types(df)
        actions.extend(coercion_actions)

        state.cleaned_df = df
        state.cleaning_actions = actions

        state.log(
            self.name,
            f"Applied {len(actions)} cleaning actions. "
            f"Shape: {df.shape[0]:,} rows × {df.shape[1]} cols",
            Severity.INFO,
        )
        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning=f"Applied {len(actions)} cleaning actions resulting in {df.shape[0]} rows.",
            data_summary={"actions_applied": len(actions)}
        )

    @staticmethod
    def _normalize_columns(df: pl.DataFrame) -> tuple[pl.DataFrame, CleaningAction | None]:
        original = df.columns
        new_names = {}
        for col in original:
            # lowercase, replace spaces/special chars with underscore, strip edges
            clean = re.sub(r"[^a-z0-9_]", "_", col.strip().lower())
            clean = re.sub(r"_+", "_", clean).strip("_")
            if not clean:
                clean = "column"
            new_names[col] = clean

        # Handle name collisions
        seen: dict[str, int] = {}
        final_names = {}
        for orig, clean in new_names.items():
            if clean in seen:
                seen[clean] += 1
                final_names[orig] = f"{clean}_{seen[clean]}"
            else:
                seen[clean] = 0
                final_names[orig] = clean

        renamed = sum(1 for o, n in final_names.items() if o != n)
        if renamed == 0:
            return df, None

        df = df.rename(final_names)
        return df, CleaningAction(
            action="normalize_columns",
            column=None,
            detail=f"Normalized {renamed} column names to snake_case",
            rows_affected=0,
        )

    @staticmethod
    def _drop_duplicates(df: pl.DataFrame) -> tuple[pl.DataFrame, CleaningAction | None]:
        before = df.shape[0]
        df = df.unique()
        dropped = before - df.shape[0]
        if dropped == 0:
            return df, None

        return df, CleaningAction(
            action="drop_duplicates",
            column=None,
            detail=f"Removed {dropped:,} duplicate rows",
            rows_affected=dropped,
        )

    @staticmethod
    def _handle_missing(
        df: pl.DataFrame, cfg: dict
    ) -> tuple[pl.DataFrame, list[CleaningAction]]:
        actions = []
        numeric_strategy = cfg.get("numeric_fill_strategy", "median")
        categorical_strategy = cfg.get("categorical_fill_strategy", "mode")

        for col_name in df.columns:
            col = df[col_name]
            null_count = col.null_count()
            if null_count == 0:
                continue

            if col.dtype.is_numeric():
                df, action = CleaningAgent._fill_numeric(
                    df, col_name, null_count, numeric_strategy
                )
            else:
                df, action = CleaningAgent._fill_categorical(
                    df, col_name, null_count, categorical_strategy
                )
            if action:
                actions.append(action)

        return df, actions

    @staticmethod
    def _fill_numeric(
        df: pl.DataFrame, col_name: str, null_count: int, strategy: str
    ) -> tuple[pl.DataFrame, CleaningAction | None]:
        if strategy == "drop":
            df = df.drop_nulls(subset=[col_name])
            return df, CleaningAction(
                action="drop_null_rows",
                column=col_name,
                detail=f"Dropped {null_count} rows with null '{col_name}'",
                rows_affected=null_count,
            )

        if strategy == "mean":
            fill_val = df[col_name].mean()
        elif strategy == "median":
            fill_val = df[col_name].median()
        elif strategy == "zero":
            fill_val = 0
        else:
            return df, None

        if fill_val is not None:
            df = df.with_columns(pl.col(col_name).fill_null(fill_val))
            return df, CleaningAction(
                action=f"fill_null_{strategy}",
                column=col_name,
                detail=f"Filled {null_count} nulls in '{col_name}' with {strategy} ({fill_val:.2f})",
                rows_affected=null_count,
            )
        return df, None

    @staticmethod
    def _fill_categorical(
        df: pl.DataFrame, col_name: str, null_count: int, strategy: str
    ) -> tuple[pl.DataFrame, CleaningAction | None]:
        if strategy == "drop":
            df = df.drop_nulls(subset=[col_name])
            return df, CleaningAction(
                action="drop_null_rows",
                column=col_name,
                detail=f"Dropped {null_count} rows with null '{col_name}'",
                rows_affected=null_count,
            )

        if strategy == "mode":
            try:
                mode_val = df[col_name].drop_nulls().mode().to_list()
                fill_val = mode_val[0] if mode_val else "Unknown"
            except Exception:
                fill_val = "Unknown"
        elif strategy == "unknown":
            fill_val = "Unknown"
        else:
            return df, None

        df = df.with_columns(pl.col(col_name).fill_null(pl.lit(fill_val)))
        return df, CleaningAction(
            action=f"fill_null_{strategy}",
            column=col_name,
            detail=f"Filled {null_count} nulls in '{col_name}' with '{fill_val}'",
            rows_affected=null_count,
        )

    @staticmethod
    def _coerce_types(df: pl.DataFrame) -> tuple[pl.DataFrame, list[CleaningAction]]:
        """Attempt to cast string columns to numeric or date where possible."""
        actions = []

        for col_name in df.columns:
            col = df[col_name]
            if col.dtype != pl.Utf8:
                continue

            # Try numeric
            try:
                non_null = col.drop_nulls()
                if len(non_null) == 0:
                    continue
                casted = non_null.cast(pl.Float64, strict=True)
                if casted.null_count() == 0:
                    df = df.with_columns(pl.col(col_name).cast(pl.Float64, strict=False))
                    actions.append(CleaningAction(
                        action="type_coercion",
                        column=col_name,
                        detail=f"Cast '{col_name}' from String → Float64",
                    ))
                    continue
            except Exception:
                pass

        return df, actions
