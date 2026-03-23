"""Shared data models for the analysis pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path


class Severity(str, Enum):
    """Log severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    SUCCESS = "success"


class FileFormat(str, Enum):
    """Supported input file formats."""
    CSV = "csv"
    XLSX = "xlsx"
    XLS = "xls"
    JSON = "json"
    PARQUET = "parquet"

    @classmethod
    def from_extension(cls, path: Path) -> FileFormat:
        ext = path.suffix.lower().lstrip(".")
        try:
            return cls(ext)
        except ValueError:
            supported = ", ".join(f.value for f in cls)
            raise ValueError(
                f"Unsupported file format '.{ext}'. Supported: {supported}"
            )


@dataclass
class LogEntry:
    """A single log entry from an agent."""
    agent: str
    message: str
    severity: Severity = Severity.INFO
    timestamp: datetime = field(default_factory=datetime.now)

    def __str__(self) -> str:
        icon = {"info": "ℹ️", "warning": "⚠️", "error": "❌", "success": "✅"}
        return f"{icon.get(self.severity.value, '•')} [{self.agent}] {self.message}"


@dataclass
class ColumnProfile:
    """Profile of a single column."""
    name: str
    dtype: str
    null_count: int
    null_pct: float
    unique_count: int
    # Numeric stats (None if non-numeric)
    mean: float | None = None
    median: float | None = None
    std: float | None = None
    min: float | None = None
    max: float | None = None
    # Categorical stats (None if numeric)
    top_values: list[tuple[str, int]] | None = None


@dataclass
class CleaningAction:
    """Record of a cleaning action performed."""
    action: str
    column: str | None
    detail: str
    rows_affected: int = 0


from pydantic import BaseModel
from typing import Literal

class AgentResult(BaseModel):
    """Structured response from every agent for the Orchestrator."""
    agent_name: str
    status: Literal["success", "skipped", "error"]
    reasoning: str
    data_summary: dict | None = None
