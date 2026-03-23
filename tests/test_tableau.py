import pytest
import polars as pl
from analyst.agents.tableau_export import TableauExportAgent
from analyst.state import AnalysisState


class TestTableauExportAgent:
    def test_generates_hyper_extract(self, tmp_path):
        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        state = AnalysisState(file_path=tmp_path / "data.csv", raw_df=df, cleaned_df=df)
        state.output_dir = tmp_path
        state.config = {}

        agent = TableauExportAgent()
        state, result = agent.execute(state)

        # Skip the test if pantab is missing
        if result.status == "skipped":
            pytest.skip("pantab not installed")

        assert result.status == "success"
        hyper_file = tmp_path / "data.hyper"
        assert hyper_file.exists()
        assert hyper_file.stat().st_size > 0

    def test_no_data_raises(self, tmp_path):
        state = AnalysisState(file_path=tmp_path / "data.csv")
        agent = TableauExportAgent()
        state, result = agent.execute(state)
        
        assert result.status == "error"
        assert "No data available" in result.reasoning
