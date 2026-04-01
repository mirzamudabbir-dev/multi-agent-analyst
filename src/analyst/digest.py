"""State Digest Engine — compressed dataset fingerprint for LLM consumption + disk cache."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from analyst.state import AnalysisState
from analyst.models import ColumnProfile, CleaningAction

CACHE_DIR = Path(".analyst_cache")


def compute_file_hash(file_path: Path) -> str:
    """Compute MD5 hash of a file for change detection."""
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def generate_digest(state: AnalysisState) -> str:
    """Compress profile + EDA into a ~150 token text string for LLM prompts."""
    lines = [f"Dataset: {state.file_path.name} ({state.row_count}×{state.col_count})"]

    numeric_cols = []
    categorical_cols = []
    for p in state.profile:
        dtype = str(p.dtype)
        info = f"{p.name}({dtype},{p.unique_count}u"
        if p.null_count > 0:
            info += f",{p.null_count}n"
        info += ")"
        if dtype in ("Float64", "Int64", "Float32", "Int32"):
            if p.mean is not None:
                info += f"[μ={p.mean:.1f}]"
            numeric_cols.append(info)
        else:
            categorical_cols.append(info)

    if numeric_cols:
        lines.append(f"Numeric: {', '.join(numeric_cols)}")
    if categorical_cols:
        lines.append(f"Categorical: {', '.join(categorical_cols)}")

    # EDA summary
    if state.eda_results:
        outliers = state.eda_results.get("outliers", {})
        if outliers:
            outlier_info = ", ".join(f"{k}({v})" for k, v in outliers.items() if v)
            if outlier_info:
                lines.append(f"Outliers: {outlier_info}")

        corr = state.eda_results.get("correlation", {})
        if corr:
            # Only top correlations
            pairs = []
            for col, corrs in corr.items():
                if isinstance(corrs, dict):
                    for col2, val in corrs.items():
                        if col < col2 and abs(val) > 0.3:
                            pairs.append(f"{col}↔{col2}({val:.2f})")
            if pairs:
                lines.append(f"Correlations: {', '.join(pairs[:5])}")

    # Cleaning summary
    if state.cleaning_actions:
        actions = ", ".join(f"{a.action}({a.column})" for a in state.cleaning_actions[:5])
        lines.append(f"Cleaning: {actions}")

    return "\n".join(lines)


def _serialize_profile(profile: list[ColumnProfile]) -> list[dict]:
    """Convert ColumnProfile list to JSON-safe dicts."""
    return [
        {
            "name": p.name,
            "dtype": str(p.dtype),
            "null_count": p.null_count,
            "null_pct": p.null_pct,
            "unique_count": p.unique_count,
            "mean": p.mean,
            "median": p.median,
            "std": p.std,
            "min": p.min,
            "max": p.max,
            "top_values": p.top_values,
        }
        for p in profile
    ]


def _serialize_cleaning(actions: list[CleaningAction]) -> list[dict]:
    return [{"action": a.action, "column": a.column, "detail": a.detail} for a in actions]


def save_cache(state: AnalysisState) -> Path:
    """Save the state digest + metadata to disk."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    file_hash = compute_file_hash(state.file_path)
    digest = generate_digest(state)

    cache_data = {
        "file_hash": file_hash,
        "file_name": state.file_path.name,
        "file_path": str(state.file_path.absolute()),
        "created_at": datetime.now().isoformat(),
        "row_count": state.row_count,
        "col_count": state.col_count,
        "digest": digest,
        "profile": _serialize_profile(state.profile),
        "eda_results": state.eda_results or {},
        "cleaning_actions": _serialize_cleaning(state.cleaning_actions),
    }

    cache_file = CACHE_DIR / f"{file_hash}.json"
    cache_file.write_text(json.dumps(cache_data, indent=2, default=str))

    # Save cleaned dataframe if available
    df_path = CACHE_DIR / f"{file_hash}.parquet"
    try:
        active = state.active_df
        if active is not None:
            active.write_parquet(df_path)
    except Exception as e:
        pass  # non-fatal if parquet fails

    # Also store digest on the state object
    state.digest = digest

    return cache_file


def load_cache(file_path: Path) -> AnalysisState | None:
    """Check if a valid cache exists for this file. Returns a hydrated state or None."""
    if not file_path.exists():
        return None

    file_hash = compute_file_hash(file_path)
    cache_file = CACHE_DIR / f"{file_hash}.json"

    if not cache_file.exists():
        return None

    try:
        cache_data = json.loads(cache_file.read_text())
    except (json.JSONDecodeError, OSError):
        return None

    # Verify hash still matches (paranoia check)
    if cache_data.get("file_hash") != file_hash:
        return None

    # Reconstruct a minimal AnalysisState from cached data
    from analyst.config import load_config

    cfg = load_config(None)
    state = AnalysisState(
        file_path=file_path,
        output_dir=Path("output"),
        config=cfg,
    )

    state.row_count = cache_data["row_count"]
    state.col_count = cache_data["col_count"]
    state.digest = cache_data["digest"]

    # Reconstruct profile
    state.profile = [
        ColumnProfile(
            name=p["name"],
            dtype=p["dtype"],
            null_count=p["null_count"],
            null_pct=p.get("null_pct", 0.0),
            unique_count=p["unique_count"],
            mean=p.get("mean"),
            median=p.get("median"),
            std=p.get("std"),
            min=p.get("min"),
            max=p.get("max"),
            top_values=p.get("top_values"),
        )
        for p in cache_data.get("profile", [])
    ]

    # Restore EDA results
    state.eda_results = cache_data.get("eda_results", {})

    # Restore cleaning actions
    state.cleaning_actions = [
        CleaningAction(action=a["action"], column=a["column"], detail=a["detail"])
        for a in cache_data.get("cleaning_actions", [])
    ]

    # Load cached dataframe if available
    df_path = CACHE_DIR / f"{file_hash}.parquet"
    if df_path.exists():
        import polars as pl
        try:
            state.cleaned_df = pl.read_parquet(df_path)
        except Exception:
            pass

    return state
