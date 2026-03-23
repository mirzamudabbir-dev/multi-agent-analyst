"""Configuration loading and defaults."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any


DEFAULT_CONFIG: dict[str, Any] = {
    "output_dir": "output",
    "cleaning": {
        "drop_duplicates": True,
        "normalize_columns": True,
        "numeric_fill_strategy": "median",   # mean | median | zero | drop
        "categorical_fill_strategy": "mode",  # mode | unknown | drop
    },
    "eda": {
        "correlation_method": "pearson",      # pearson | spearman
        "outlier_method": "iqr",              # iqr | zscore
        "outlier_threshold": 1.5,
    },
    "visualization": {
        "format": "png",                      # png | html | both
        "max_categories": 20,
        "figsize": [10, 6],
        "style": "seaborn-v0_8-darkgrid",
    },
    "reporting": {
        "format": "markdown",                 # markdown | html | both
        "include_charts": True,
    },
}


def load_config(config_path: Path | None = None) -> dict[str, Any]:
    """
    Load configuration from a TOML file, merged over defaults.

    If no path is given, looks for `analyst.toml` in the current directory.
    Missing keys fall back to DEFAULT_CONFIG.
    """
    config = _deep_copy_dict(DEFAULT_CONFIG)

    if config_path is None:
        config_path = Path("analyst.toml")

    if config_path.exists():
        with open(config_path, "rb") as f:
            user_config = tomllib.load(f)
        config = _deep_merge(config, user_config)

    return config


def _deep_copy_dict(d: dict) -> dict:
    """Simple deep copy for nested dicts/lists."""
    out = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out[k] = _deep_copy_dict(v)
        elif isinstance(v, list):
            out[k] = list(v)
        else:
            out[k] = v
    return out


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base."""
    merged = _deep_copy_dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged
