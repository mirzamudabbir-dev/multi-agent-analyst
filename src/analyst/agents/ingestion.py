"""Ingestion Agent — loads data from CSV, XLSX, JSON, Parquet into Polars."""

from __future__ import annotations

import polars as pl

from analyst.base_agent import BaseAgent
from analyst.models import FileFormat, Severity, AgentResult
from analyst.registry import register_agent
from analyst.state import AnalysisState


@register_agent("ingestion")
class IngestionAgent(BaseAgent):
    """Reads the input file and populates state.raw_df."""

    name = "ingestion"
    description = "Data ingestion"

    def validate_preconditions(self, state: AnalysisState) -> None:
        if not state.file_path.exists():
            raise FileNotFoundError(f"Input file not found: {state.file_path}")

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        fmt = FileFormat.from_extension(state.file_path)
        state.log(self.name, f"Detected format: {fmt.value}")

        loader = self._get_loader(fmt)
        state.raw_df = loader(state.file_path)
        state.row_count = state.raw_df.shape[0]
        state.col_count = state.raw_df.shape[1]

        state.log(
            self.name,
            f"Loaded {state.row_count:,} rows × {state.col_count} columns",
            Severity.INFO,
        )
        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning=f"Loaded {state.row_count} rows × {state.col_count} columns from {fmt.value}",
            data_summary={"rows": state.row_count, "cols": state.col_count}
        )

    def _get_loader(self, fmt: FileFormat):
        loaders = {
            FileFormat.CSV: self._load_csv,
            FileFormat.JSON: self._load_json,
            FileFormat.PARQUET: self._load_parquet,
            FileFormat.XLSX: self._load_xlsx,
            FileFormat.XLS: self._load_xlsx,
        }
        return loaders[fmt]

    @staticmethod
    def _load_csv(path) -> pl.DataFrame:
        return pl.read_csv(path, infer_schema_length=10000, try_parse_dates=True)

    @staticmethod
    def _load_json(path) -> pl.DataFrame:
        return pl.read_json(path)

    @staticmethod
    def _load_parquet(path) -> pl.DataFrame:
        return pl.read_parquet(path)

    @staticmethod
    def _load_xlsx(path) -> pl.DataFrame:
        import openpyxl

        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        ws = wb.active
        data = list(ws.iter_rows(values_only=True))
        wb.close()

        if not data:
            return pl.DataFrame()

        headers = [str(h) if h is not None else f"column_{i}" for i, h in enumerate(data[0])]
        rows = data[1:]
        return pl.DataFrame(dict(zip(headers, zip(*rows))), orient="col", strict=False)
