"""Tests for the Ingestion Agent."""

from pathlib import Path

import polars as pl
import pytest

from analyst.agents.ingestion import IngestionAgent
from analyst.state import AnalysisState


class TestIngestionAgent:

    def test_load_csv(self, sample_csv: Path):
        state = AnalysisState(file_path=sample_csv)
        agent = IngestionAgent()
        state, result = agent.execute(state)

        assert state.raw_df is not None
        assert state.raw_df.shape[0] == 10
        assert state.raw_df.shape[1] == 5
        assert state.row_count == 10
        assert state.col_count == 5

    def test_load_json(self, sample_json: Path):
        state = AnalysisState(file_path=sample_json)
        agent = IngestionAgent()
        state, result = agent.execute(state)

        assert state.raw_df is not None
        assert state.raw_df.shape[0] == 3
        assert "name" in state.raw_df.columns

    def test_load_xlsx(self, sample_xlsx: Path):
        state = AnalysisState(file_path=sample_xlsx)
        agent = IngestionAgent()
        state, result = agent.execute(state)

        assert state.raw_df is not None
        assert state.raw_df.shape[0] == 3
        assert "Product" in state.raw_df.columns

    def test_load_parquet(self, sample_parquet: Path):
        state = AnalysisState(file_path=sample_parquet)
        agent = IngestionAgent()
        state, result = agent.execute(state)

        assert state.raw_df is not None
        assert state.raw_df.shape[0] == 5
        assert "id" in state.raw_df.columns

    def test_file_not_found(self, tmp_path: Path):
        state = AnalysisState(file_path=tmp_path / "nonexistent.csv")
        agent = IngestionAgent()

        _, result = agent.execute(state)

        assert result.status == "error"

        if "" != "None":

            assert "" in result.reasoning

    def test_unsupported_format(self, tmp_path: Path):
        bad_file = tmp_path / "data.xyz"
        bad_file.write_text("hello")
        state = AnalysisState(file_path=bad_file)
        agent = IngestionAgent()

        _, result = agent.execute(state)

        assert result.status == "error"

        if "Unsupported file format" != "None":

            assert "Unsupported file format" in result.reasoning
