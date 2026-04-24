"""Local settings for development utilities."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from game_market_analytics.paths import ProjectPaths, get_project_paths


@dataclass(frozen=True)
class LocalSettings:
    """Runtime settings derived from environment variables and repository defaults."""

    paths: ProjectPaths
    duckdb_path: Path


def _resolve_repo_path(value: str, project_root: Path) -> Path:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = project_root / candidate
    return candidate.resolve()


def load_local_settings(paths: ProjectPaths | None = None) -> LocalSettings:
    """Load local settings without requiring a .env parser."""

    resolved_paths = paths or get_project_paths()
    duckdb_env_value = os.getenv("DUCKDB_PATH")
    duckdb_path = (
        _resolve_repo_path(duckdb_env_value, resolved_paths.project_root)
        if duckdb_env_value
        else resolved_paths.duckdb_path
    )

    return LocalSettings(paths=resolved_paths, duckdb_path=duckdb_path)
