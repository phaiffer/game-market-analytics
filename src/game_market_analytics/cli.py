"""Development CLI for local repository utilities."""

from __future__ import annotations

import argparse
from pathlib import Path

from game_market_analytics.config import LocalSettings, load_local_settings
from game_market_analytics.ingestion.steam.app_catalog import ingest_steam_app_catalog
from game_market_analytics.ingestion.steam.client import SteamClientError
from game_market_analytics.ingestion.steam.stage_app_catalog import (
    SteamAppCatalogStageError,
    stage_steam_app_catalog,
)


def _format_path(path: Path) -> str:
    return str(path)


def _print_paths(settings: LocalSettings) -> None:
    paths = settings.paths
    print(f"project_root={_format_path(paths.project_root)}")
    print(f"data_raw={_format_path(paths.raw_data_dir)}")
    print(f"data_stage={_format_path(paths.stage_data_dir)}")
    print(f"data_marts={_format_path(paths.marts_data_dir)}")
    print(f"dbt_dir={_format_path(paths.dbt_dir)}")
    print(f"local_dir={_format_path(paths.local_dir)}")
    print(f"duckdb_path={_format_path(settings.duckdb_path)}")


def _validate_project(settings: LocalSettings) -> int:
    paths = settings.paths
    missing = [path for path in paths.required_directories if not path.exists()]

    if missing:
        print("Project validation failed. Missing required directories:")
        for path in missing:
            print(f"- {_format_path(path)}")
        print("Run `game-market-analytics init-local` to create local writable folders.")
        return 1

    print("Project validation passed.")
    print("Required scaffold directories are present.")
    print(f"Future DuckDB path: {_format_path(settings.duckdb_path)}")
    return 0


def _init_local(settings: LocalSettings) -> int:
    for directory in settings.paths.writable_directories:
        directory.mkdir(parents=True, exist_ok=True)

    print("Local writable directories are ready.")
    _print_paths(settings)
    return 0


def _ingest_steam_app_catalog(settings: LocalSettings) -> int:
    for directory in settings.paths.writable_directories:
        directory.mkdir(parents=True, exist_ok=True)

    try:
        result = ingest_steam_app_catalog(
            paths=settings.paths,
            steam_api_key=settings.steam_api_key,
        )
    except SteamClientError as exc:
        print(f"Steam app catalog ingestion failed: {exc}")
        return 1

    print("Steam app catalog ingestion completed.")
    print(f"app_count={result.app_count}")
    print(f"output_file_path={_format_path(result.output_file_path)}")
    print(f"metadata_file_path={_format_path(result.metadata_file_path)}")
    return 0


def _stage_steam_app_catalog(settings: LocalSettings, raw_file: str | None = None) -> int:
    for directory in settings.paths.writable_directories:
        directory.mkdir(parents=True, exist_ok=True)

    try:
        result = stage_steam_app_catalog(
            paths=settings.paths,
            raw_file_path=Path(raw_file) if raw_file else None,
        )
    except SteamAppCatalogStageError as exc:
        print(f"Steam app catalog staging failed: {exc}")
        return 1

    print("Steam app catalog staging completed.")
    print(f"raw_input_path={_format_path(result.raw_input_path)}")
    print(f"staged_output_path={_format_path(result.staged_output_path)}")
    print(f"metadata_file_path={_format_path(result.metadata_file_path)}")
    print(f"row_count={result.row_count}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="game-market-analytics",
        description="Local development utilities for the Game Market Analytics project.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "validate-project",
        help="Validate that the local repository scaffold is present.",
    )
    subparsers.add_parser(
        "show-paths",
        help="Print important repository paths used by local utilities.",
    )
    subparsers.add_parser(
        "init-local",
        help="Create local writable folders used during development.",
    )
    subparsers.add_parser(
        "ingest-steam-app-catalog",
        help="Fetch and land the raw Steam app catalog payload.",
    )
    stage_parser = subparsers.add_parser(
        "stage-steam-app-catalog",
        help="Normalize a raw Steam app catalog extract into staged Parquet.",
    )
    stage_parser.add_argument(
        "--raw-file",
        help="Optional path to a specific raw app_catalog.json file.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = load_local_settings()

    if args.command == "validate-project":
        return _validate_project(settings)
    if args.command == "show-paths":
        _print_paths(settings)
        return 0
    if args.command == "init-local":
        return _init_local(settings)
    if args.command == "ingest-steam-app-catalog":
        return _ingest_steam_app_catalog(settings)
    if args.command == "stage-steam-app-catalog":
        return _stage_steam_app_catalog(settings, raw_file=args.raw_file)

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
