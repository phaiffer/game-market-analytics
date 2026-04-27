"""Local settings for development utilities."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from game_market_analytics.paths import ProjectPaths, get_project_paths

SteamApiKeyAuthLocation = Literal["query", "header"]


@dataclass(frozen=True)
class LocalSettings:
    """Runtime settings derived from environment variables and repository defaults."""

    paths: ProjectPaths
    duckdb_path: Path
    steam_api_key: str | None = None
    steam_api_key_auth_location: SteamApiKeyAuthLocation = "query"


def _resolve_repo_path(value: str, project_root: Path) -> Path:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = project_root / candidate
    return candidate.resolve()


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")

    return values


def _get_setting(name: str, env_file_values: dict[str, str]) -> str | None:
    return os.getenv(name) or env_file_values.get(name) or None


def _steam_auth_location(value: str | None) -> SteamApiKeyAuthLocation:
    normalized = (value or "query").strip().lower()
    if normalized not in ("query", "header"):
        raise ValueError("STEAM_API_KEY_AUTH_LOCATION must be either 'query' or 'header'.")
    return normalized


def load_local_settings(paths: ProjectPaths | None = None) -> LocalSettings:
    """Load local settings without requiring a .env parser."""

    resolved_paths = paths or get_project_paths()
    env_file_values = _load_env_file(resolved_paths.project_root / ".env")

    duckdb_env_value = _get_setting("DUCKDB_PATH", env_file_values)
    duckdb_path = (
        _resolve_repo_path(duckdb_env_value, resolved_paths.project_root)
        if duckdb_env_value
        else resolved_paths.duckdb_path
    )
    steam_api_key = _get_setting("STEAM_API_KEY", env_file_values)
    steam_api_key_auth_location = _steam_auth_location(
        _get_setting("STEAM_API_KEY_AUTH_LOCATION", env_file_values)
    )

    return LocalSettings(
        paths=resolved_paths,
        duckdb_path=duckdb_path,
        steam_api_key=steam_api_key,
        steam_api_key_auth_location=steam_api_key_auth_location,
    )
