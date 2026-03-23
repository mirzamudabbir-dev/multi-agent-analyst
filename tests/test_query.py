import os
import pytest
from unittest.mock import patch, MagicMock

from analyst.state import AnalysisState
from analyst.query_agent import ask_question


class TestQueryAgent:
    @patch("google.genai.Client")
    @patch.dict(os.environ, {"GEMINI_API_KEY": "test-key"})
    def test_mock_query(self, mock_client_cls, tmp_path):
        # Setup mock
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.text = "Average salary is $75,000."
        mock_client.models.generate_content.return_value = mock_response
        mock_client_cls.return_value = mock_client

        # Dummy state
        state = AnalysisState(file_path=tmp_path / "data.csv")
        state.eda_results = {"numeric_summary": {"salary": {"mean": 75000}}}

        answer = ask_question(state, "What is the average salary?")
        
        assert answer == "Average salary is $75,000."
        mock_client.models.generate_content.assert_called_once()
        
    def test_missing_api_key(self, tmp_path):
        with patch.dict(os.environ, {}, clear=True):
            state = AnalysisState(file_path=tmp_path / "data.csv")
            answer = ask_question(state, "test")
            assert "GEMINI_API_KEY environment variable is not set" in answer
