"""Central analysis state shared across all agents."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from pydantic import BaseModel, ConfigDict, Field

from analyst.event_emitter import global_emitter
from analyst.models import CleaningAction, ColumnProfile, LogEntry, Severity


@dataclass
class AnalysisState:
    """
    The shared state object that flows through the pipeline.

    Every agent reads from and writes to this object.
    No direct agent-to-agent coupling — all communication
    goes through state.
    """

    # ── Input ──────────────────────────────────────────────
    file_path: Path
    output_dir: Path = field(default_factory=lambda: Path("output"))
    api_key: str | None = None

    # ── Ingestion output ───────────────────────────────────
    raw_df: pl.DataFrame | None = None
    row_count: int = 0
    col_count: int = 0

    # ── Profiling output ───────────────────────────────────
    profile: list[ColumnProfile] = field(default_factory=list)
    memory_usage_bytes: int = 0

    # ── Cleaning output ────────────────────────────────────
    cleaned_df: pl.DataFrame | None = None
    cleaning_actions: list[CleaningAction] = field(default_factory=list)

    # ── EDA output ─────────────────────────────────────────
    eda_results: dict = field(default_factory=dict)

    # ── Visualization output ───────────────────────────────
    visualizations: list[Path] = field(default_factory=list)
    charts_dir: Path | None = None

    # ── Reporting output ───────────────────────────────────
    report_path: Path | None = None

    # ── Pipeline metadata ──────────────────────────────────
    logs: list[LogEntry] = field(default_factory=list)
    config: dict = field(default_factory=dict)
    skipped_agents: list[str] = field(default_factory=list)
    failed_agents: list[str] = field(default_factory=list)

    # ── Digest (compressed text for LLM) ──────────────
    digest: str = ""

    def log(
        self, agent: str, message: str, severity: Severity = Severity.INFO
    ) -> None:
        """Append a log entry."""
        self.logs.append(LogEntry(agent=agent, message=message, severity=severity))
        global_emitter.emit("log", {"agent": agent, "message": message, "severity": severity.name})

    @property
    def active_df(self) -> pl.DataFrame | None:
        """Return the most processed dataframe available."""
        return self.cleaned_df if self.cleaned_df is not None else self.raw_df

    def ensure_output_dirs(self) -> None:
        """Create output directories if they don't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.charts_dir = self.output_dir / "charts"
        self.charts_dir.mkdir(parents=True, exist_ok=True)
