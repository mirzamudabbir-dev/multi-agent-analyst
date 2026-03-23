"""Deterministic Pipeline Runner + Legacy LLM Orchestrator."""

from __future__ import annotations

import json
import os
import hashlib
from typing import Any

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from analyst.models import AgentResult, Severity
from analyst.state import AnalysisState
from analyst.registry import get_agent, get_all_agents

console = Console()

# ─── Deterministic Pipeline (default, 0 routing tokens) ──────────

PIPELINE = [
    # Phase 1: Data (local Python)
    "ingestion",
    "profiling",
    "cleaning",
    "eda",
    # >>> DIGEST CHECKPOINT <<<
    # Phase 2: Reasoning (LLM, uses digest)
    "strategic_planner",
    "data_reasoner",
    # Phase 3: Execution (local Python)
    "query_execution",
    "transformation",
    # Phase 2b: Final Reasoning (LLM)
    "synthesis_expert",
    # Phase 3b: Final Execution (local Python)
    "visualization_execution",
    "reporting_execution",
    "excel_export",
]

DIGEST_CHECKPOINT = "eda"  # Generate + save digest after this agent


class DeterministicPipeline:
    """Runs agents in a fixed order. Zero LLM routing calls."""

    def __init__(self, state: AnalysisState, skip_up_to: str | None = None):
        self.state = state
        self.skip_up_to = skip_up_to  # Skip agents up to (and including) this name (for cache hits)

    def run(self) -> AnalysisState:
        from analyst.digest import generate_digest, save_cache

        skipping = self.skip_up_to is not None

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            main_task = progress.add_task("[bold cyan]Pipeline running...[/]", total=None)

            for agent_name in PIPELINE:
                # Skip agents whose output is already cached
                if skipping:
                    if agent_name == self.skip_up_to:
                        skipping = False
                    continue

                agent = get_agent(agent_name)
                if agent is None:
                    continue

                progress.update(main_task, description=f"[bold yellow]Running {agent_name}[/]")

                try:
                    self.state, result = agent.execute(self.state)
                except Exception as e:
                    result = AgentResult(
                        agent_name=agent_name,
                        status="error",
                        reasoning=str(e),
                    )

                # Display result
                if result.status == "success":
                    icon = "✅"
                elif result.status == "skipped":
                    icon = "⚠️"
                else:
                    icon = "❌"
                console.print(f" {icon} [bold]{agent_name}[/]: {result.reasoning}")

                # If an LLM agent fails, continue — don't crash the whole pipeline
                if result.status == "error":
                    self.state.failed_agents.append(agent_name)
                    continue

                # Generate digest + save cache right after EDA
                if agent_name == DIGEST_CHECKPOINT:
                    self.state.digest = generate_digest(self.state)
                    save_cache(self.state)

                    # Post-phase validation: abort reasoning if data is empty
                    if self.state.row_count == 0:
                        progress.update(main_task, description="[bold red]Dataset empty after cleaning. Skipping reasoning.[/]")
                        break

            progress.update(main_task, description="[bold green]Pipeline complete![/]")

        return self.state


# ─── Legacy LLM Orchestrator (--smart mode) ──────────────────────

class LLMOrchestrator:
    """Uses an LLM to dynamically plan and execute specialist agents."""

    def __init__(self, state: AnalysisState, goal: str = "Analyze this dataset fully"):
        self.state = state
        self.goal = goal
        self.history: list[AgentResult] = []
        self._cache: dict[str, dict] = {}
        self.available_agents = list(get_all_agents().keys())
        self.api_key = os.environ.get("GEMINI_API_KEY")

    def run(self) -> AnalysisState:
        """Main loop: plan → execute → loop until complete."""
        try:
            from google import genai
            from pydantic import BaseModel
        except ImportError:
            raise ImportError("google-genai and pydantic packages are required for the Orchestrator.")

        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable is missing.")

        client = genai.Client(api_key=self.api_key)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            main_task = progress.add_task("[bold cyan]Orchestrator thinking...[/]", total=None)

            while True:
                import time
                time.sleep(1.0)

                progress.update(main_task, description="[bold cyan]Orchestrator thinking...[/]")
                plan = self._plan_next(client)

                if plan.get("is_complete", False):
                    progress.update(main_task, description=f"[bold green]Analysis complete![/] reason: {plan.get('reasoning')}")
                    break

                next_agent = plan.get("next_agent")
                if next_agent not in self.available_agents:
                    progress.update(main_task, description=f"[bold red]LLM hallucinated agent '{next_agent}'. Ending loop.[/]")
                    break

                reasoning = plan.get("reasoning", "Executing next agent")
                progress.update(main_task, description=f"[bold yellow]Running {next_agent}[/] ([dim]{reasoning}[/])")

                agent = get_agent(next_agent)
                self.state, result = agent.execute(self.state)
                self.history.append(result)

                icon = "✅" if result.status == "success" else ("⚠️" if result.status == "skipped" else "❌")
                console.print(f" {icon} [bold]{next_agent}[/]: {result.reasoning}")

        return self.state

    def _plan_next(self, client: Any) -> dict:
        """Calls the LLM to get the next agent to run."""
        prompt = self._build_prompt()
        prompt_hash = hashlib.md5(prompt.encode("utf-8")).hexdigest()

        if prompt_hash in self._cache:
            return self._cache[prompt_hash]

        try:
            from google import genai
            from pydantic import BaseModel

            class PlanSchema(BaseModel):
                next_agent: str | None
                reasoning: str
                is_complete: bool

            import time
            max_retries = 3
            backoff_delay = 2

            for attempt in range(max_retries):
                try:
                    response = client.models.generate_content(
                        model="gemini-2.5-flash",
                        contents=prompt,
                        config=genai.types.GenerateContentConfig(
                            response_mime_type="application/json",
                            response_schema=PlanSchema,
                            temperature=0.0,
                        ),
                    )
                    content = response.text
                    plan = json.loads(content)
                    self._cache[prompt_hash] = plan
                    return plan
                except Exception as e:
                    error_str = str(e).lower()
                    if "429" in error_str or "resource_exhausted" in error_str:
                        if attempt < max_retries - 1:
                            time.sleep(backoff_delay)
                            backoff_delay *= 2
                            continue
                    raise e
        except Exception as e:
            return {
                "next_agent": None,
                "reasoning": f"LLM Error: {e}",
                "is_complete": True
            }

    def _build_prompt(self) -> str:
        # Compressed history: one line per agent instead of full JSON dump
        history_lines = [f"  {h.agent_name}: {h.status}" for h in self.history]
        history_str = "\n".join(history_lines) if history_lines else "  (none)"

        return f"""You are an Orchestrator AI for a Multi-Agent Data Analysis tool.
Available Agents: {self.available_agents}

User Goal: {self.goal}
File: {self.state.file_path.name}

Execution History:
{history_str}

Determine if the goal is achieved. If it is, return is_complete: true.
If not, determine which agent needs to run NEXT.
Do not repeat an agent unless absolutely necessary.
If ingestion is not in history, you MUST run ingestion first.

Return JSON: {{"next_agent": "string", "reasoning": "string", "is_complete": boolean}}
"""
