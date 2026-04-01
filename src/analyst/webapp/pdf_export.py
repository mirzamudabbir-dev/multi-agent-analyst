"""PDF Report Generator — builds a downloadable analysis report."""

from __future__ import annotations

from pathlib import Path
from fpdf import FPDF
from analyst.state import AnalysisState


class AnalysisReportPDF(FPDF):
    """Custom PDF with dark-themed header/footer."""

    def header(self):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(120, 120, 140)
        self.cell(0, 8, "ANALYST_OS // MULTI-AGENT INTELLIGENCE REPORT", align="L")
        self.ln(4)
        self.set_draw_color(100, 60, 200)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(6)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 160)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")


def generate_pdf(state: AnalysisState, qa_history: list[dict] | None = None) -> bytes:
    """
    Generate a PDF report from the analysis state.

    Args:
        state: The completed AnalysisState with profile, EDA, report, etc.
        qa_history: Optional list of {"question": ..., "answer": ...} dicts.

    Returns:
        PDF file content as bytes.
    """
    pdf = AnalysisReportPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # ─── Title ──────────────────────────────────────────────
    pdf.set_font("Helvetica", "B", 24)
    pdf.set_text_color(40, 40, 50)
    pdf.cell(0, 14, "Data Analysis Report", ln=True, align="C")
    pdf.ln(2)

    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(100, 100, 120)
    if state.file_path:
        pdf.cell(0, 7, f"Dataset: {state.file_path.name}", ln=True, align="C")
    pdf.cell(0, 7, f"Rows: {state.row_count:,}  |  Columns: {state.col_count}", ln=True, align="C")
    pdf.ln(8)

    # ─── 1. Data Profile ───────────────────────────────────
    _section_header(pdf, "01", "DATA PROFILE")

    if state.profile:
        # Table header
        col_widths = [42, 22, 18, 20, 28, 28, 28]
        headers = ["Column", "Type", "Nulls", "Unique", "Mean", "Median", "Std"]

        pdf.set_font("Helvetica", "B", 9)
        pdf.set_fill_color(235, 235, 245)
        pdf.set_text_color(40, 40, 50)
        for w, h in zip(col_widths, headers):
            pdf.cell(w, 7, h, border=1, fill=True, align="C")
        pdf.ln()

        # Table rows
        pdf.set_font("Helvetica", "", 8)
        for i, p in enumerate(state.profile):
            fill = i % 2 == 0
            if fill:
                pdf.set_fill_color(248, 248, 252)
            pdf.set_text_color(50, 50, 60)

            pdf.cell(col_widths[0], 6, _trunc(p.name, 22), border=1, fill=fill)
            pdf.cell(col_widths[1], 6, str(p.dtype), border=1, fill=fill, align="C")
            pdf.cell(col_widths[2], 6, str(p.null_count), border=1, fill=fill, align="C")
            pdf.cell(col_widths[3], 6, str(p.unique_count), border=1, fill=fill, align="C")
            pdf.cell(col_widths[4], 6, _fmt_num(p.mean), border=1, fill=fill, align="R")
            pdf.cell(col_widths[5], 6, _fmt_num(p.median), border=1, fill=fill, align="R")
            pdf.cell(col_widths[6], 6, _fmt_num(p.std), border=1, fill=fill, align="R")
            pdf.ln()
    else:
        pdf.set_font("Helvetica", "I", 10)
        pdf.set_text_color(140, 140, 150)
        pdf.cell(0, 8, "No profile data available.", ln=True)

    pdf.ln(6)

    # ─── 2. EDA Summary ────────────────────────────────────
    if state.eda_results:
        _section_header(pdf, "02", "EXPLORATORY DATA ANALYSIS")

        # Outliers
        outliers = state.eda_results.get("outliers", {})
        if outliers:
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(60, 60, 70)
            pdf.cell(0, 7, f"Outlier Detection ({len(outliers)} column(s)):", ln=True)
            pdf.set_font("Helvetica", "", 8)
            for col, info in outliers.items():
                count = info.get("count", "?")
                pct = info.get("pct", "?")
                pdf.set_text_color(80, 80, 90)
                pdf.cell(0, 5, f"  - {col}: {count} outliers ({pct}%)", ln=True)
            pdf.ln(3)

        # Correlations
        corr = state.eda_results.get("correlation", {})
        if corr:
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(60, 60, 70)
            pdf.cell(0, 7, "Correlation Matrix:", ln=True)
            pdf.set_font("Helvetica", "", 8)
            for col_pair, value in list(corr.items())[:10]:
                pdf.set_text_color(80, 80, 90)
                pdf.cell(0, 5, f"  - {col_pair}: {value:.4f}" if isinstance(value, (int, float)) else f"  - {col_pair}: {value}", ln=True)
            pdf.ln(3)

        pdf.ln(6)

    # ─── 3. Charts ─────────────────────────────────────────
    chart_files = [p for p in state.visualizations if p.exists()]
    if chart_files:
        _section_header(pdf, "03", "VISUALIZATIONS")

        for chart_path in chart_files:
            try:
                # Check remaining space on page
                if pdf.get_y() > 200:
                    pdf.add_page()

                pdf.set_font("Helvetica", "I", 8)
                pdf.set_text_color(120, 120, 140)
                pdf.cell(0, 5, chart_path.stem.replace("_", " ").title(), ln=True)

                img_w = 170
                # Omitting 'x' and 'y' forces fpdf2 to treat the image as a flowing block, automatically 
                # advancing the vertical position (get_y()) by the exact height of the image so nothing overlaps.
                pdf.image(str(chart_path), w=img_w)
                pdf.ln(8)
            except Exception:
                pdf.set_font("Helvetica", "I", 8)
                pdf.set_text_color(180, 80, 80)
                pdf.cell(0, 5, f"  [Could not embed: {chart_path.name}]", ln=True)

        pdf.ln(4)

    # ─── 4. Executive Report ───────────────────────────────
    report_text = None
    if state.report_path and state.report_path.exists():
        report_text = state.report_path.read_text().strip()
    elif state.eda_results and state.eda_results.get("final_markdown_report"):
        report_text = state.eda_results["final_markdown_report"]

    if report_text:
        _section_header(pdf, "04", "EXECUTIVE REPORT")
        _render_markdown_text(pdf, report_text)
        pdf.ln(6)

    # ─── 5. Q&A History ────────────────────────────────────
    if qa_history:
        _section_header(pdf, "05", "NATURAL LANGUAGE Q&A")

        for i, qa in enumerate(qa_history, 1):
            if pdf.get_y() > 250:
                pdf.add_page()

            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(100, 60, 200)
            q_text = str(qa.get('question', ''))
            # Truncate question in header to avoid cell overflow crash
            pdf.cell(0, 7, f"Q{i}: {q_text[:90]}..." if len(q_text) > 90 else f"Q{i}: {q_text}", ln=True)

            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(50, 50, 60)
            answer = qa.get("answer", "No answer.")
            pdf.multi_cell(0, 5, _safe_wrap(answer))
            
            pdf.ln(4)

    out = pdf.output(dest="S")
    if isinstance(out, str):
        return out.encode("latin-1", errors="replace")
    return bytes(out)


# ─── Helpers ──────────────────────────────────────────────


def _section_header(pdf: FPDF, idx: str, title: str):
    """Render a styled section header."""
    if pdf.get_y() > 240:
        pdf.add_page()

    pdf.set_draw_color(100, 60, 200)
    pdf.set_line_width(0.3)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(3)

    pdf.set_font("Helvetica", "", 8)
    pdf.set_text_color(100, 60, 200)
    pdf.cell(0, 5, f"{idx} /", ln=True)

    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(40, 40, 50)
    pdf.cell(0, 9, title, ln=True)
    pdf.ln(4)


def _trunc(text: str, maxlen: int) -> str:
    return text if len(text) <= maxlen else text[:maxlen - 3] + "..."


def _fmt_num(val) -> str:
    if val is None:
        return "-"
    try:
        return f"{float(val):.2f}"
    except (ValueError, TypeError):
        return str(val)


def _safe_wrap(text: str, max_word_len: int = 60) -> str:
    """Proactively insert spaces into massive continuous strings to prevent fpdf2 buffer corruption."""
    return " ".join([w if len(w) <= max_word_len else w[:max_word_len] + " " + w[max_word_len:] for w in text.split(" ")])

def _render_markdown_text(pdf: FPDF, text: str):
    """Simple Markdown-to-PDF text renderer (handles headers, bullets, paragraphs)."""
    pdf.set_text_color(50, 50, 60)

    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped:
            pdf.ln(3)
            continue

        stripped = _safe_wrap(stripped)

        if stripped.startswith("### "):
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_text_color(60, 60, 70)
            pdf.cell(0, 6, stripped[4:], ln=True)
        elif stripped.startswith("## "):
            pdf.set_font("Helvetica", "B", 12)
            pdf.set_text_color(50, 50, 60)
            pdf.cell(0, 7, stripped[3:], ln=True)
        elif stripped.startswith("# "):
            pdf.set_font("Helvetica", "B", 14)
            pdf.set_text_color(40, 40, 50)
            pdf.cell(0, 9, stripped[2:], ln=True)
        elif stripped.startswith("- ") or stripped.startswith("* "):
            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(70, 70, 80)
            pdf.cell(8)
            pdf.cell(0, 5, f"-  {stripped[2:]}", ln=True)
        elif stripped.startswith("**") and stripped.endswith("**"):
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(50, 50, 60)
            pdf.multi_cell(0, 5, stripped.replace("**", ""))
        else:
            pdf.set_font("Helvetica", "", 9)
            pdf.set_text_color(70, 70, 80)
            pdf.multi_cell(0, 5, stripped)
