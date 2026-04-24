"""Development CLI for local repository utilities."""

from __future__ import annotations

import argparse
from pathlib import Path

from game_market_analytics.config import LocalSettings, load_local_settings
from game_market_analytics.ingestion.steam.app_catalog import ingest_steam_app_catalog
from game_market_analytics.ingestion.steam.client import SteamClientError
from game_market_analytics.ingestion.steam.reviews import (
    DEFAULT_FILTER,
    DEFAULT_LANGUAGE,
    DEFAULT_MAX_PAGES,
    DEFAULT_REVIEW_TYPE,
    SteamReviewsInputError,
    ingest_reviews_batch,
    parse_app_ids,
)
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


def _ingest_steam_reviews(
    settings: LocalSettings,
    *,
    app_id_values: list[str] | None,
    input_file: str | None,
    max_pages: int,
    language: str,
    review_type: str,
    filter_value: str,
) -> int:
    for directory in settings.paths.writable_directories:
        directory.mkdir(parents=True, exist_ok=True)

    try:
        app_ids = parse_app_ids(
            app_id_values=app_id_values,
            input_file=Path(input_file) if input_file else None,
        )
        if max_pages <= 0:
            raise SteamReviewsInputError("--max-pages must be a positive integer.")
    except SteamReviewsInputError as exc:
        print(f"Steam reviews ingestion input error: {exc}")
        return 2

    results = ingest_reviews_batch(
        paths=settings.paths,
        app_ids=app_ids,
        max_pages=max_pages,
        language=language,
        review_type=review_type,
        filter_value=filter_value,
    )

    print("Steam reviews ingestion completed.")
    for result in results:
        print(
            " ".join(
                [
                    f"app_id={result.app_id}",
                    f"status={result.status}",
                    f"pages_fetched={result.pages_fetched}",
                    f"review_count={result.review_count}",
                    f"stop_reason={result.pagination_stop_reason}",
                    f"metadata_file_path={_format_path(result.metadata_file_path)}",
                ]
            )
        )

    return 1 if any(result.status != "success" for result in results) else 0


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
    reviews_parser = subparsers.add_parser(
        "ingest-steam-reviews",
        help="Fetch and land raw Steam reviews for a controlled set of app IDs.",
    )
    reviews_parser.add_argument(
        "--app-id",
        action="append",
        dest="app_ids",
        help="Steam app ID to ingest. Can be repeated.",
    )
    reviews_parser.add_argument(
        "--input-file",
        help="Text file with one Steam app ID per line.",
    )
    reviews_parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=f"Maximum review pages to fetch per app ID. Default: {DEFAULT_MAX_PAGES}.",
    )
    reviews_parser.add_argument(
        "--language",
        default=DEFAULT_LANGUAGE,
        help=f"Steam review language code or 'all'. Default: {DEFAULT_LANGUAGE}.",
    )
    reviews_parser.add_argument(
        "--review-type",
        default=DEFAULT_REVIEW_TYPE,
        choices=["all", "positive", "negative"],
        help=f"Review type filter. Default: {DEFAULT_REVIEW_TYPE}.",
    )
    reviews_parser.add_argument(
        "--filter",
        default=DEFAULT_FILTER,
        choices=["recent", "updated", "all"],
        help=f"Steam review sort/filter mode. Default: {DEFAULT_FILTER}.",
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
    if args.command == "ingest-steam-reviews":
        return _ingest_steam_reviews(
            settings,
            app_id_values=args.app_ids,
            input_file=args.input_file,
            max_pages=args.max_pages,
            language=args.language,
            review_type=args.review_type,
            filter_value=args.filter,
        )

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
