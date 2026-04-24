"""Development CLI for local repository utilities."""

from __future__ import annotations

import argparse
from pathlib import Path

from game_market_analytics.config import LocalSettings, load_local_settings


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

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
