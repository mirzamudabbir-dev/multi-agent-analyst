"""Tests for the Profiling Agent."""

import polars as pl
import pytest

from analyst.agents.profiling import ProfilingAgent
from analyst.state import AnalysisState


class TestProfilingAgent:

    def test_profiles_all_columns(self, sample_df: pl.DataFrame, tmp_path):
        state = AnalysisState(file_path=tmp_path / "dummy.csv")
        state.raw_df = sample_df
        agent = ProfilingAgent()
        state, result = agent.execute(state)

        assert len(state.profile) == 4
        names = [p.name for p in state.profile]
        assert "name" in names
        assert "age" in names
        assert "salary" in names
        assert "department" in names

    def test_numeric_stats(self, sample_df: pl.DataFrame, tmp_path):
        state = AnalysisState(file_path=tmp_path / "dummy.csv")
        state.raw_df = sample_df
        agent = ProfilingAgent()
        state, result = agent.execute(state)

        age_profile = next(p for p in state.profile if p.name == "age")
        assert age_profile.mean is not None
        assert age_profile.median is not None
        assert age_profile.std is not None
        assert age_profile.min == 25.0
        assert age_profile.max == 35.0
        assert age_profile.top_values is None  # numeric col

    def test_categorical_top_values(self, sample_df: pl.DataFrame, tmp_path):
        state = AnalysisState(file_path=tmp_path / "dummy.csv")
        state.raw_df = sample_df
        agent = ProfilingAgent()
        state, result = agent.execute(state)

        dept_profile = next(p for p in state.profile if p.name == "department")
        assert dept_profile.top_values is not None
        assert len(dept_profile.top_values) > 0
        assert dept_profile.mean is None  # non-numeric

    def test_null_detection(self, sample_df: pl.DataFrame, tmp_path):
        state = AnalysisState(file_path=tmp_path / "dummy.csv")
        state.raw_df = sample_df
        agent = ProfilingAgent()
        state, result = agent.execute(state)

        salary_profile = next(p for p in state.profile if p.name == "salary")
        assert salary_profile.null_count == 1
        assert salary_profile.null_pct == 20.0

    def test_memory_usage(self, sample_df: pl.DataFrame, tmp_path):
        state = AnalysisState(file_path=tmp_path / "dummy.csv")
        state.raw_df = sample_df
        agent = ProfilingAgent()
        state, result = agent.execute(state)

        assert state.memory_usage_bytes > 0

    def test_no_data_raises(self, tmp_path):
        state = AnalysisState(file_path=tmp_path / "dummy.csv")
        agent = ProfilingAgent()

        _, result = agent.execute(state)

        assert result.status == "error"

        if "No data loaded" != "None":

            assert "No data loaded" in result.reasoning
