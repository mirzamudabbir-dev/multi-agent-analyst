import pytest
from analyst.agents.reporting import ReportingExecutionAgent
from analyst.state import AnalysisState

class TestReportingExecutionAgent:
    def test_saves_markdown_report(self, tmp_path):
        state = AnalysisState(file_path=tmp_path / "data.csv")
        state.output_dir = tmp_path
        state.eda_results = {"final_markdown_report": "# Test Report\nEverything is fine."}

        agent = ReportingExecutionAgent()
        state, result = agent.execute(state)

        assert result.status == "success"
        report_file = tmp_path / "report.md"
        assert report_file.exists()
        assert report_file.read_text() == "# Test Report\nEverything is fine."
