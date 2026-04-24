from pathlib import Path

from game_market_analytics.config import load_local_settings
from game_market_analytics.paths import ProjectPaths


def test_load_local_settings_uses_default_duckdb_path(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("DUCKDB_PATH", raising=False)

    paths = ProjectPaths(project_root=tmp_path)
    settings = load_local_settings(paths)

    assert settings.duckdb_path == tmp_path / ".local" / "game_market_analytics.duckdb"


def test_load_local_settings_resolves_relative_duckdb_path(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("DUCKDB_PATH", "data/custom.duckdb")

    paths = ProjectPaths(project_root=tmp_path)
    settings = load_local_settings(paths)

    assert settings.duckdb_path == (tmp_path / "data" / "custom.duckdb").resolve()
