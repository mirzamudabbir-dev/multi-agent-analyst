import polars as pl
from analyst.agents.excel_export import ExcelExportAgent
from analyst.state import AnalysisState

class TestExcelExportAgent:
    def test_generates_dashboard(self, tmp_path):
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        state = AnalysisState(file_path=tmp_path / "data.csv")
        state.raw_df = df
        state.output_dir = tmp_path
        state.eda_results = {
            "chart_instructions": [{"type": "bar", "x": "b", "y": "a", "title": "Test"}]
        }

        agent = ExcelExportAgent()
        state, result = agent.execute(state)

        # Skip the test if xlsxwriter is missing
        if result.status == "skipped":
            import pytest
            pytest.skip("xlsxwriter not installed")

        assert result.status == "success"
        excel_file = tmp_path / "data_dashboard.xlsx"
        assert excel_file.exists()
        assert excel_file.stat().st_size > 0
