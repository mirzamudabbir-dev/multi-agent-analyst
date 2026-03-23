"""Shared fixtures for tests."""

from pathlib import Path

import polars as pl
import pytest


FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """Create a sample CSV file with mixed column types."""
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(
        "Name,Age,Salary,Department,Start Date\n"
        "Alice,30,75000.50,Engineering,2020-01-15\n"
        "Bob,25,65000.00,Marketing,2021-03-22\n"
        "Charlie,35,85000.75,Engineering,2019-06-01\n"
        "Diana,28,,Marketing,2022-01-10\n"
        "Eve,32,90000.00,Engineering,2020-09-05\n"
        "Frank,,72000.00,Sales,2021-07-14\n"
        "Grace,29,68000.00,Marketing,2022-04-01\n"
        "Hank,45,120000.00,Engineering,2018-02-20\n"
        "Ivy,26,63000.00,Sales,2023-01-03\n"
        "Jack,30,75000.50,Engineering,2020-01-15\n"  # duplicate of Alice
    )
    return csv_path


@pytest.fixture
def sample_json(tmp_path: Path) -> Path:
    """Create a sample JSON file."""
    import json

    data = [
        {"name": "Alice", "age": 30, "score": 85.5},
        {"name": "Bob", "age": 25, "score": 92.0},
        {"name": "Charlie", "age": 35, "score": 78.0},
    ]
    json_path = tmp_path / "sample.json"
    json_path.write_text(json.dumps(data))
    return json_path


@pytest.fixture
def sample_xlsx(tmp_path: Path) -> Path:
    """Create a sample XLSX file."""
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["Product", "Price", "Quantity"])
    ws.append(["Widget A", 10.50, 100])
    ws.append(["Widget B", 25.00, 50])
    ws.append(["Widget C", 5.75, 200])

    xlsx_path = tmp_path / "sample.xlsx"
    wb.save(xlsx_path)
    return xlsx_path


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    """Create a sample Parquet file."""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "value": [10.5, 20.3, 30.1, 40.7, 50.2],
        "category": ["A", "B", "A", "C", "B"],
    })
    parquet_path = tmp_path / "sample.parquet"
    df.write_parquet(parquet_path)
    return parquet_path


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """A sample Polars DataFrame for unit tests."""
    return pl.DataFrame({
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [30, 25, 35, 28, 32],
        "salary": [75000.0, 65000.0, 85000.0, None, 90000.0],
        "department": ["Engineering", "Marketing", "Engineering", "Marketing", "Engineering"],
    })
