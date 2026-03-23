"""Tests for the EDA Agent."""

import polars as pl
import pytest

from analyst.agents.eda import EDAAgent
from analyst.state import AnalysisState


class TestEDAAgent:

    def _make_state(self, df: pl.DataFrame, tmp_path):
        state = AnalysisState(file_path=tmp_path / "dummy.csv")
        state.cleaned_df = df
        return state

    def test_numeric_summary(self, tmp_path):
        df = pl.DataFrame({
            "a": [1.0, 2.0, 3.0, 4.0, 5.0],
            "b": [10.0, 20.0, 30.0, 40.0, 50.0],
        })
        state = self._make_state(df, tmp_path)
        agent = EDAAgent()
        state, result = agent.execute(state)

        ns = state.eda_results["numeric_summary"]
        assert "a" in ns
        assert ns["a"]["mean"] == 3.0
        assert ns["a"]["min"] == 1.0
        assert ns["a"]["max"] == 5.0

    def test_correlation(self, tmp_path):
        df = pl.DataFrame({
            "x": [1.0, 2.0, 3.0, 4.0, 5.0],
            "y": [2.0, 4.0, 6.0, 8.0, 10.0],  # perfect correlation
        })
        state = self._make_state(df, tmp_path)
        agent = EDAAgent()
        state, result = agent.execute(state)

        corr = state.eda_results["correlation"]
        assert corr["x"]["y"] == pytest.approx(1.0, abs=1e-3)
        assert corr["y"]["x"] == pytest.approx(1.0, abs=1e-3)

    def test_outlier_detection(self, tmp_path):
        # Most values around 10, one extreme outlier
        df = pl.DataFrame({"val": [10.0, 11.0, 10.5, 9.5, 10.2, 100.0]})
        state = self._make_state(df, tmp_path)
        agent = EDAAgent()
        state, result = agent.execute(state)

        outliers = state.eda_results["outliers"]
        assert outliers["val"]["count"] >= 1

    def test_categorical_summary(self, tmp_path):
        df = pl.DataFrame({
            "color": ["red", "blue", "red", "green", "blue", "red"],
            "size": [1, 2, 3, 4, 5, 6],
        })
        state = self._make_state(df, tmp_path)
        agent = EDAAgent()
        state, result = agent.execute(state)

        cs = state.eda_results["categorical_summary"]
        assert "color" in cs
        assert cs["color"]["unique_count"] == 3

    def test_no_data_raises(self, tmp_path):
        state = AnalysisState(file_path=tmp_path / "dummy.csv")
        agent = EDAAgent()

        _, result = agent.execute(state)

        assert result.status == "error"

        if "No data available" != "None":

            assert "No data available" in result.reasoning
