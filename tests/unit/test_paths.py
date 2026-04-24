from pathlib import Path

from game_market_analytics.paths import ProjectPaths, get_project_paths


def test_project_paths_resolve_expected_directories() -> None:
    paths = get_project_paths()

    assert paths.project_root.name == "game-market-analytics"
    assert paths.raw_data_dir == paths.project_root / "data" / "raw"
    assert paths.stage_data_dir == paths.project_root / "data" / "stage"
    assert paths.marts_data_dir == paths.project_root / "data" / "marts"
    assert paths.dbt_dir == paths.project_root / "dbt"
    assert paths.duckdb_path == paths.project_root / ".local" / "game_market_analytics.duckdb"


def test_project_paths_can_be_built_from_custom_root(tmp_path: Path) -> None:
    paths = ProjectPaths(project_root=tmp_path)

    assert paths.raw_data_dir == tmp_path / "data" / "raw"
    assert paths.local_dir == tmp_path / ".local"
    assert paths.duckdb_path == tmp_path / ".local" / "game_market_analytics.duckdb"
