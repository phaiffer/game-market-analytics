"""Normalize raw Steam review extracts into staged Parquet datasets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from game_market_analytics.ingestion.steam.app_catalog import (
    SOURCE_NAME,
    extract_date_from_run_timestamp,
)
from game_market_analytics.ingestion.steam.reviews import EXTRACTION_TYPE
from game_market_analytics.paths import ProjectPaths


STAGE_COLUMNS = {
    "source_system": "VARCHAR",
    "source_app_id": "BIGINT",
    "review_id": "VARCHAR",
    "review_text": "VARCHAR",
    "language": "VARCHAR",
    "review_score_desc": "VARCHAR",
    "voted_up": "BOOLEAN",
    "votes_up": "BIGINT",
    "votes_funny": "BIGINT",
    "weighted_vote_score": "DOUBLE",
    "steam_purchase": "BOOLEAN",
    "received_for_free": "BOOLEAN",
    "written_during_early_access": "BOOLEAN",
    "author_steamid": "VARCHAR",
    "author_num_games_owned": "BIGINT",
    "author_num_reviews": "BIGINT",
    "review_created_at": "TIMESTAMP",
    "review_updated_at": "TIMESTAMP",
    "extract_date": "VARCHAR",
    "run_timestamp": "VARCHAR",
    "raw_file_path": "VARCHAR",
    "ingestion_status": "VARCHAR",
}


class SteamReviewsStageError(RuntimeError):
    """Raised when a Steam reviews extract cannot be staged."""


@dataclass(frozen=True)
class RawSteamReviewsExtract:
    """Raw review extract selected for stage normalization."""

    app_id: int
    raw_run_dir: Path
    page_file_paths: tuple[Path, ...]
    metadata_file_path: Path | None
    extract_date: str
    run_timestamp: str
    ingestion_status: str


@dataclass(frozen=True)
class SteamReviewsStageResult:
    """Summary of one app-specific Steam reviews staging run."""

    app_id: int
    source_name: str
    transformation_type: str
    raw_input_paths: tuple[Path, ...]
    staged_output_path: Path
    metadata_file_path: Path
    run_timestamp: str
    row_count: int
    pages_processed: int
    status: str


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SteamReviewsStageError(f"Raw file does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise SteamReviewsStageError(f"Raw file is not valid JSON: {path}") from exc

    if not isinstance(payload, dict):
        raise SteamReviewsStageError(f"Raw file must contain a JSON object: {path}")

    return payload


def _partition_value(path: Path, prefix: str) -> str | None:
    for part in path.parts:
        if part.startswith(prefix):
            return part.removeprefix(prefix)
    return None


def _integer_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _float_or_none(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _timestamp_or_none(value: Any) -> datetime | None:
    timestamp = _integer_or_none(value)
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, UTC).replace(tzinfo=None)


def _resolve_raw_path(path_value: str, *, paths: ProjectPaths, raw_run_dir: Path) -> Path:
    candidate = Path(path_value)
    if candidate.is_absolute():
        return candidate

    project_candidate = paths.project_root / candidate
    if project_candidate.exists():
        return project_candidate

    return raw_run_dir / candidate


def _app_id_from_path(path: Path) -> int | None:
    value = _partition_value(path, "app_id=")
    return int(value) if value and value.isdigit() else None


def build_reviews_stage_run_dir(stage_steam_dir: Path, app_id: int, run_timestamp: str) -> Path:
    """Build the staged landing directory for one Steam reviews run."""

    extract_date = extract_date_from_run_timestamp(run_timestamp)
    return (
        stage_steam_dir
        / EXTRACTION_TYPE
        / f"app_id={app_id}"
        / f"extract_date={extract_date}"
        / f"run_timestamp={run_timestamp}"
    )


def build_raw_reviews_extract(raw_path: Path, *, paths: ProjectPaths) -> RawSteamReviewsExtract:
    """Build raw review extract metadata for a raw run directory or page file."""

    raw_run_dir = raw_path if raw_path.is_dir() else raw_path.parent
    metadata_file_path = raw_run_dir / "metadata.json"
    metadata = _read_json(metadata_file_path) if metadata_file_path.exists() else {}

    app_id = _integer_or_none(metadata.get("app_id")) or _app_id_from_path(raw_run_dir)
    if app_id is None:
        raise SteamReviewsStageError("Could not determine app_id from raw metadata or path.")

    run_timestamp = (
        str(metadata.get("run_timestamp"))
        if metadata.get("run_timestamp")
        else _partition_value(raw_run_dir, "run_timestamp=")
    )
    if not run_timestamp:
        raise SteamReviewsStageError("Could not determine run_timestamp from raw metadata or path.")

    extract_date = (
        _partition_value(raw_run_dir, "extract_date=")
        or extract_date_from_run_timestamp(run_timestamp)
    )
    ingestion_status = str(metadata.get("status", "unknown"))

    output_files = metadata.get("output_files")
    if isinstance(output_files, list) and output_files:
        page_file_paths = tuple(
            path
            for path in (
                _resolve_raw_path(str(value), paths=paths, raw_run_dir=raw_run_dir)
                for value in output_files
            )
            if path.exists()
        )
    elif raw_path.is_file() and raw_path.name.startswith("reviews_page_"):
        page_file_paths = (raw_path,)
    else:
        page_file_paths = tuple(sorted(raw_run_dir.glob("reviews_page_*.json")))

    if not page_file_paths:
        raise SteamReviewsStageError(f"No raw Steam review page files found under: {raw_run_dir}")

    return RawSteamReviewsExtract(
        app_id=app_id,
        raw_run_dir=raw_run_dir,
        page_file_paths=page_file_paths,
        metadata_file_path=metadata_file_path if metadata_file_path.exists() else None,
        extract_date=extract_date,
        run_timestamp=run_timestamp,
        ingestion_status=ingestion_status,
    )


def find_latest_successful_raw_review_extracts(
    paths: ProjectPaths,
    *,
    app_id: int | None = None,
) -> list[RawSteamReviewsExtract]:
    """Find the latest successful raw Steam review extract for each selected app ID."""

    raw_root = paths.raw_data_dir / SOURCE_NAME / EXTRACTION_TYPE
    latest_by_app_id: dict[int, RawSteamReviewsExtract] = {}

    for metadata_file_path in raw_root.glob("app_id=*/extract_date=*/run_timestamp=*/metadata.json"):
        metadata = _read_json(metadata_file_path)
        if metadata.get("status") != "success":
            continue

        candidate = build_raw_reviews_extract(metadata_file_path.parent, paths=paths)
        if app_id is not None and candidate.app_id != app_id:
            continue

        current = latest_by_app_id.get(candidate.app_id)
        if current is None or candidate.run_timestamp > current.run_timestamp:
            latest_by_app_id[candidate.app_id] = candidate

    if not latest_by_app_id:
        app_filter = f" for app_id={app_id}" if app_id is not None else ""
        raise SteamReviewsStageError(f"No successful Steam reviews raw extract was found{app_filter}.")

    return [latest_by_app_id[key] for key in sorted(latest_by_app_id)]


def normalize_review_records(
    payload: dict[str, Any],
    *,
    extract: RawSteamReviewsExtract,
    raw_file_path: Path,
) -> list[dict[str, Any]]:
    """Normalize one raw Steam review page into stage-shaped rows."""

    reviews = payload.get("reviews")
    if not isinstance(reviews, list):
        return []

    query_summary = payload.get("query_summary")
    review_score_desc = (
        _string_or_none(query_summary.get("review_score_desc"))
        if isinstance(query_summary, dict)
        else None
    )

    rows: list[dict[str, Any]] = []
    for review in reviews:
        if not isinstance(review, dict):
            continue
        author = review.get("author") if isinstance(review.get("author"), dict) else {}
        rows.append(
            {
                "source_system": SOURCE_NAME,
                "source_app_id": extract.app_id,
                "review_id": _string_or_none(review.get("recommendationid")),
                "review_text": _string_or_none(review.get("review")),
                "language": _string_or_none(review.get("language")),
                "review_score_desc": review_score_desc,
                "voted_up": _bool_or_none(review.get("voted_up")),
                "votes_up": _integer_or_none(review.get("votes_up")),
                "votes_funny": _integer_or_none(review.get("votes_funny")),
                "weighted_vote_score": _float_or_none(review.get("weighted_vote_score")),
                "steam_purchase": _bool_or_none(review.get("steam_purchase")),
                "received_for_free": _bool_or_none(review.get("received_for_free")),
                "written_during_early_access": _bool_or_none(
                    review.get("written_during_early_access")
                ),
                "author_steamid": _string_or_none(author.get("steamid")),
                "author_num_games_owned": _integer_or_none(author.get("num_games_owned")),
                "author_num_reviews": _integer_or_none(author.get("num_reviews")),
                "review_created_at": _timestamp_or_none(review.get("timestamp_created")),
                "review_updated_at": _timestamp_or_none(review.get("timestamp_updated")),
                "extract_date": extract.extract_date,
                "run_timestamp": extract.run_timestamp,
                "raw_file_path": str(raw_file_path),
                "ingestion_status": extract.ingestion_status,
            }
        )

    return rows


def write_reviews_stage_parquet(output_path: Path, rows: list[dict[str, Any]]) -> None:
    """Write stage review rows as Parquet using DuckDB."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    column_sql = ", ".join(f"{name} {data_type}" for name, data_type in STAGE_COLUMNS.items())

    with duckdb.connect(database=":memory:") as connection:
        connection.execute(f"CREATE TABLE stage_reviews ({column_sql})")
        if rows:
            placeholders = ", ".join("?" for _ in STAGE_COLUMNS)
            values = [tuple(row.get(column) for column in STAGE_COLUMNS) for row in rows]
            connection.executemany(f"INSERT INTO stage_reviews VALUES ({placeholders})", values)
        connection.execute("COPY stage_reviews TO ? (FORMAT PARQUET)", [str(output_path)])


def build_reviews_stage_metadata(
    *,
    extract: RawSteamReviewsExtract,
    staged_output_path: Path,
    metadata_file_path: Path,
    row_count: int,
    pages_processed: int,
    status: str,
) -> dict[str, Any]:
    """Build operational metadata for Steam reviews stage normalization."""

    return {
        "source_name": SOURCE_NAME,
        "transformation_type": f"stage_{EXTRACTION_TYPE}",
        "app_id": extract.app_id,
        "raw_input_paths": [str(path) for path in extract.page_file_paths],
        "raw_run_dir": str(extract.raw_run_dir),
        "staged_output_path": str(staged_output_path),
        "metadata_file_path": str(metadata_file_path),
        "row_count": row_count,
        "pages_processed": pages_processed,
        "run_timestamp": extract.run_timestamp,
        "status": status,
        "schema": [{"name": name, "type": data_type} for name, data_type in STAGE_COLUMNS.items()],
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def stage_steam_reviews_extract(
    *,
    paths: ProjectPaths,
    extract: RawSteamReviewsExtract,
) -> SteamReviewsStageResult:
    """Normalize one raw Steam reviews extract into staged Parquet."""

    rows: list[dict[str, Any]] = []
    for page_file_path in extract.page_file_paths:
        payload = _read_json(page_file_path)
        rows.extend(
            normalize_review_records(
                payload,
                extract=extract,
                raw_file_path=page_file_path,
            )
        )

    stage_run_dir = build_reviews_stage_run_dir(
        paths.stage_data_dir / SOURCE_NAME,
        extract.app_id,
        extract.run_timestamp,
    )
    staged_output_path = stage_run_dir / "reviews.parquet"
    metadata_file_path = stage_run_dir / "metadata.json"

    write_reviews_stage_parquet(staged_output_path, rows)
    metadata = build_reviews_stage_metadata(
        extract=extract,
        staged_output_path=staged_output_path,
        metadata_file_path=metadata_file_path,
        row_count=len(rows),
        pages_processed=len(extract.page_file_paths),
        status="success",
    )
    write_json(metadata_file_path, metadata)

    return SteamReviewsStageResult(
        app_id=extract.app_id,
        source_name=SOURCE_NAME,
        transformation_type=f"stage_{EXTRACTION_TYPE}",
        raw_input_paths=extract.page_file_paths,
        staged_output_path=staged_output_path,
        metadata_file_path=metadata_file_path,
        run_timestamp=extract.run_timestamp,
        row_count=len(rows),
        pages_processed=len(extract.page_file_paths),
        status="success",
    )


def stage_steam_reviews(
    *,
    paths: ProjectPaths,
    app_id: int | None = None,
    raw_path: Path | None = None,
) -> list[SteamReviewsStageResult]:
    """Stage latest successful raw Steam review extracts or a selected raw path."""

    extracts = (
        [build_raw_reviews_extract(raw_path, paths=paths)]
        if raw_path
        else find_latest_successful_raw_review_extracts(paths, app_id=app_id)
    )
    return [stage_steam_reviews_extract(paths=paths, extract=extract) for extract in extracts]
