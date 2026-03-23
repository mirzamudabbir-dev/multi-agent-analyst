"""Tests for the Cleaning Agent."""

import polars as pl
import pytest

from analyst.agents.cleaning import CleaningAgent
from analyst.state import AnalysisState


class TestCleaningAgent:

    def _make_state(self, df: pl.DataFrame, tmp_path, config: dict | None = None):
        state = AnalysisState(file_path=tmp_path / "dummy.csv")
        state.raw_df = df
        state.config = config or {}
        return state

    def test_drop_duplicates(self, tmp_path):
        df = pl.DataFrame({
            "a": [1, 2, 2, 3],
            "b": ["x", "y", "y", "z"],
        })
        state = self._make_state(df, tmp_path)
        agent = CleaningAgent()
        state, result = agent.execute(state)

        assert state.cleaned_df is not None
        assert state.cleaned_df.shape[0] == 3
        dup_actions = [a for a in state.cleaning_actions if a.action == "drop_duplicates"]
        assert len(dup_actions) == 1
        assert dup_actions[0].rows_affected == 1

    def test_normalize_columns(self, tmp_path):
        df = pl.DataFrame({
            "First Name": ["Alice"],
            "Last   Name": ["Smith"],
            "AGE": [30],
        })
        state = self._make_state(df, tmp_path)
        agent = CleaningAgent()
        state, result = agent.execute(state)

        cols = state.cleaned_df.columns
        assert "first_name" in cols
        assert "last_name" in cols
        assert "age" in cols

    def test_fill_numeric_median(self, tmp_path):
        df = pl.DataFrame({"val": [10.0, 20.0, None, 40.0]})
        state = self._make_state(df, tmp_path, {"cleaning": {"numeric_fill_strategy": "median"}})
        agent = CleaningAgent()
        state, result = agent.execute(state)

        assert state.cleaned_df["val"].null_count() == 0

    def test_fill_categorical_mode(self, tmp_path):
        df = pl.DataFrame({"cat": ["A", "A", "B", None]})
        state = self._make_state(df, tmp_path, {"cleaning": {"categorical_fill_strategy": "mode"}})
        agent = CleaningAgent()
        state, result = agent.execute(state)

        assert state.cleaned_df["cat"].null_count() == 0

    def test_cleaning_actions_recorded(self, sample_df: pl.DataFrame, tmp_path):
        state = self._make_state(sample_df, tmp_path)
        agent = CleaningAgent()
        state, result = agent.execute(state)

        # Should have at least a null-fill action for salary
        assert len(state.cleaning_actions) > 0

    def test_no_data_raises(self, tmp_path):
        state = AnalysisState(file_path=tmp_path / "dummy.csv")
        agent = CleaningAgent()

        _, result = agent.execute(state)

        assert result.status == "error"

        if "No data loaded" != "None":

            assert "No data loaded" in result.reasoning
