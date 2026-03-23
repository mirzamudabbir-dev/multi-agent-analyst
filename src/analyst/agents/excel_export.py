"""Excel Export Agent - builds an Excel workbook matching the Visualization LLM charts."""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import xlsxwriter

from analyst.base_agent import BaseAgent
from analyst.models import Severity, AgentResult
from analyst.registry import register_agent
from analyst.state import AnalysisState


@register_agent("excel_export")
class ExcelExportAgent(BaseAgent):
    """Exports data and natively generated charts into an Excel Workbook."""

    name = "excel_export"
    description = "Excel Dashboard Export"

    def validate_preconditions(self, state: AnalysisState) -> None:
        if state.active_df is None:
            raise ValueError("No data available for Excel export.")

    def run(self, state: AnalysisState) -> tuple[AnalysisState, AgentResult]:
        df = state.active_df
        assert df is not None

        output_dir = state.output_dir
        output_dir.mkdir(parents=True, exist_ok=True)
        excel_path = output_dir / f"{state.file_path.stem}_dashboard.xlsx"

        try:
            import xlsxwriter
        except ImportError:
            return state, AgentResult(
                agent_name=self.name,
                status="skipped",
                reasoning="xlsxwriter package is not installed."
            )

        instructions = []
        if state.eda_results and "chart_instructions" in state.eda_results:
            instructions = state.eda_results["chart_instructions"]

        # Limit to 50,000 rows for excel export perf
        if len(df) > 50000:
            export_df = df.sample(50000)
        else:
            export_df = df

        workbook = xlsxwriter.Workbook(str(excel_path))
        data_sheet = workbook.add_worksheet("Data")
        dash_sheet = workbook.add_worksheet("Dashboard")

        # Write Data
        columns = export_df.columns
        for col_num, col_name in enumerate(columns):
            data_sheet.write(0, col_num, col_name)

        # Polars to native list
        rows = export_df.iter_rows()
        for row_num, row_data in enumerate(rows, start=1):
            for col_num, cell_value in enumerate(row_data):
                # handle None or unsupported types simply
                if cell_value is None:
                    data_sheet.write(row_num, col_num, "")
                else:
                    try:
                        data_sheet.write(row_num, col_num, cell_value)
                    except TypeError:
                        data_sheet.write(row_num, col_num, str(cell_value))

        # Render charts in Dashboard
        chart_row = 1
        charts_created = 0
        
        for inst in instructions:
            ctype = inst.get("type")
            x_col = inst.get("x")
            y_col = inst.get("y")
            title = inst.get("title")

            if not x_col or x_col not in columns:
                continue

            x_idx = columns.index(x_col)
            max_row = len(export_df)
            
            excel_chart = None
            if ctype == "histogram" or ctype == "bar":
                excel_chart = workbook.add_chart({'type': 'column'})
            elif ctype == "scatter":
                excel_chart = workbook.add_chart({'type': 'scatter'})

            if not excel_chart:
                continue

            # Add series
            if y_col and y_col in columns:
                y_idx = columns.index(y_col)
                excel_chart.add_series({
                    'name': title,
                    'categories': ['Data', 1, x_idx, max_row, x_idx],
                    'values':     ['Data', 1, y_idx, max_row, y_idx],
                })
            else:
                # If no Y, Excel's simple chart approach requires we just plot X values as sequence
                # (Excel natively struggles with frequency histograms without pivots, but we'll approximate a line chart or just plot values)
                excel_chart.add_series({
                    'name': title,
                    'values': ['Data', 1, x_idx, max_row, x_idx],
                })

            excel_chart.set_title({'name': title})
            excel_chart.set_x_axis({'name': x_col})
            if y_col:
                excel_chart.set_y_axis({'name': y_col})

            dash_sheet.insert_chart(chart_row, 1, excel_chart)
            chart_row += 16 # space out charts
            charts_created += 1

        workbook.close()

        state.log(self.name, f"Exported {excel_path.name} with {charts_created} charts.", Severity.INFO)
        return state, AgentResult(
            agent_name=self.name,
            status="success",
            reasoning=f"Generated Excel dashboard with {charts_created} embedded charts.",
            data_summary={"charts_embedded": charts_created, "excel_file": excel_path.name}
        )
