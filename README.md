# Multi-Agent Analyst

A CLI tool that orchestrates a team of specialized agents to automate data analysis pipelines.

## Architecture

```
CLI (Typer + Rich)
  │
  ▼
Orchestrator (Pipeline Engine)
  │
  ├── Ingestion Agent    → loads CSV, XLSX, JSON, Parquet
  ├── Profiling Agent    → column types, stats, nulls
  ├── Cleaning Agent     → dedup, fill nulls, coerce types
  ├── EDA Agent          → distributions, correlations, outliers
  ├── Visualization Agent → histograms, box plots, heatmaps
  └── Reporting Agent    → Markdown/HTML report
```

Each agent reads from and writes to a shared `AnalysisState` — no direct agent-to-agent coupling.

## Quick Start

```bash
# Install
uv pip install -e ".[dev]"

# Run full pipeline
analyst run data.csv

# Quick profile only
analyst profile data.csv

# Skip specific agents
analyst run data.xlsx --skip visualization --skip cleaning

# Custom output directory
analyst run data.csv --output ./results

# Use a custom config
analyst run data.csv --config my_config.toml

# List available agents
analyst agents
```

## Configuration

Copy `analyst.toml` and customize:

```toml
[cleaning]
numeric_fill_strategy = "mean"   # mean | median | zero | drop

[visualization]
format = "both"                  # png | html | both

[reporting]
format = "both"                  # markdown | html | both
```

## Output

```
output/
├── charts/
│   ├── hist_age.png
│   ├── hist_salary.png
│   ├── bar_department.png
│   ├── boxplot_overview.png
│   └── correlation_heatmap.png
└── report.md
```

## Development

```bash
# Install dev dependencies
uv pip install -e ".[dev]"

# Run tests
uv run pytest tests/ -v
```
