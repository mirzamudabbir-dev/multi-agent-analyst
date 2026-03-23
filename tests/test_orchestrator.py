import pytest
from unittest.mock import MagicMock, patch
from analyst.state import AnalysisState
from analyst.orchestrator import LLMOrchestrator
from analyst.models import AgentResult


class TestLLMOrchestrator:

    @patch("google.genai.Client")
    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    def test_orchestrator_caching(self, mock_client_cls, tmp_path):
        state = AnalysisState(file_path=tmp_path / "data.csv")
        orchestrator = LLMOrchestrator(state)

        mock_client = MagicMock()
        mock_response = MagicMock()
        # On first call, return ingestion agent. Then it will reach the loop limit or crash if agent handles it.
        # Wait, we can test _plan_next directly
        mock_response.text = '{"next_agent": "ingestion", "reasoning": "start", "is_complete": false}'
        mock_client.models.generate_content.return_value = mock_response
        mock_client_cls.return_value = mock_client

        # Call _plan_next twice without changing history
        # Since history is identical, the prompt hash is identical, so the cache should hit.
        plan1 = orchestrator._plan_next(mock_client)
        plan2 = orchestrator._plan_next(mock_client)

        assert plan1 == plan2
        assert mock_client.models.generate_content.call_count == 1
        assert "ingestion" == plan1["next_agent"]

    @patch("google.genai.Client")
    @patch.dict("os.environ", {"GEMINI_API_KEY": "test-key"})
    def test_orchestrator_loop_completion(self, mock_client_cls, tmp_path, monkeypatch):
        # We need an agent that doesn't actually do anything so the execution doesn't fail
        from analyst.base_agent import BaseAgent
        from analyst.registry import register_agent

        @register_agent("dummy")
        class DummyAgent(BaseAgent):
            name = "dummy"
            description = "dummy"
            def run(self, state):
                return state, AgentResult(agent_name="dummy", status="success", reasoning="done")

        state = AnalysisState(file_path=tmp_path / "data.csv")
        orchestrator = LLMOrchestrator(state)
        orchestrator.available_agents.append("dummy")

        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        resp1 = MagicMock()
        resp1.text = '{"next_agent": "dummy", "reasoning": "do dummy", "is_complete": false}'
        
        resp2 = MagicMock()
        resp2.text = '{"next_agent": null, "reasoning": "all done", "is_complete": true}'
        
        mock_client.models.generate_content.side_effect = [resp1, resp2]

        final_state = orchestrator.run()

        assert len(orchestrator.history) == 1
        assert orchestrator.history[0].agent_name == "dummy"
        assert mock_client.models.generate_content.call_count == 2
