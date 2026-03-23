import polars as pl
import pytest
from analyst.agents.visualization import VisualizationExecutionAgent
from analyst.state import AnalysisState

class TestVisualizationExecutionAgent:
    def test_generates_charts_from_state(self, tmp_path):
        df = pl.DataFrame({"a": [1.0, 2.0, 3.0, 4.0, 5.0], "b": [10.0, 20.0, 30.0, 40.0, 50.0], "c": ["A", "B", "A", "C", "C"]})
        state = AnalysisState(file_path=tmp_path / "data.csv")
        state.output_dir = tmp_path
        state.raw_df = df
        state.eda_results = {
            "chart_instructions": [
                {"type": "histogram", "x": "a", "title": "Hist A"},
                {"type": "bar", "x": "c", "y": "b", "title": "Bar C by B"},
                {"type": "scatter", "x": "a", "y": "b", "title": "Scatter"}
            ]
        }

        agent = VisualizationExecutionAgent()
        state, result = agent.execute(state)

        assert result.status == "success"
        assert "hist_a.png" in [p.name for p in state.visualizations]
        assert "bar_c.png" in [p.name for p in state.visualizations]
        assert "scatter_a_b.png" in [p.name for p in state.visualizations]
        assert len(state.visualizations) == 3
