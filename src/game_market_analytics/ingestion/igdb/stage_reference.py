"""Normalize raw IGDB reference extracts into staged Parquet datasets."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from game_market_analytics.ingestion.igdb.reference import (
    EXTRACTION_TYPE,
    SOURCE_NAME,
    slugify_title,
)
from game_market_analytics.ingestion.steam.app_catalog import extract_date_from_run_timestamp
from game_market_analytics.paths import ProjectPaths


ENTITY_COLUMNS: dict[str, dict[str, str]] = {
    "games": {
        "igdb_game_id": "BIGINT",
        "game_name": "VARCHAR",
        "slug": "VARCHAR",
        "first_release_date": "BIGINT",
        "aggregated_rating": "DOUBLE",
        "aggregated_rating_count": "BIGINT",
        "category": "BIGINT",
        "input_title": "VARCHAR",
        "title_slug": "VARCHAR",
        "selected_igdb_game_id": "BIGINT",
        "extract_date": "VARCHAR",
        "run_timestamp": "VARCHAR",
        "raw_file_path": "VARCHAR",
        "ingestion_status": "VARCHAR",
        "staging_status": "VARCHAR",
    },
    "involved_companies": {
        "involved_company_id": "BIGINT",
        "igdb_game_id": "BIGINT",
        "company_id": "BIGINT",
        "developer_flag": "BOOLEAN",
        "publisher_flag": "BOOLEAN",
        "supporting_flag": "BOOLEAN",
        "porting_flag": "BOOLEAN",
        "input_title": "VARCHAR",
        "title_slug": "VARCHAR",
        "selected_igdb_game_id": "BIGINT",
        "extract_date": "VARCHAR",
        "run_timestamp": "VARCHAR",
        "raw_file_path": "VARCHAR",
        "ingestion_status": "VARCHAR",
        "staging_status": "VARCHAR",
    },
    "companies": {
        "company_id": "BIGINT",
        "company_name": "VARCHAR",
        "slug": "VARCHAR",
        "country": "BIGINT",
        "start_date": "BIGINT",
        "input_title": "VARCHAR",
        "title_slug": "VARCHAR",
        "selected_igdb_game_id": "BIGINT",
        "extract_date": "VARCHAR",
        "run_timestamp": "VARCHAR",
        "raw_file_path": "VARCHAR",
        "ingestion_status": "VARCHAR",
        "staging_status": "VARCHAR",
    },
    "genres": {
        "genre_id": "BIGINT",
        "genre_name": "VARCHAR",
        "slug": "VARCHAR",
        "input_title": "VARCHAR",
        "title_slug": "VARCHAR",
        "selected_igdb_game_id": "BIGINT",
        "extract_date": "VARCHAR",
        "run_timestamp": "VARCHAR",
        "raw_file_path": "VARCHAR",
        "ingestion_status": "VARCHAR",
        "staging_status": "VARCHAR",
    },
    "platforms": {
        "platform_id": "BIGINT",
        "platform_name": "VARCHAR",
        "slug": "VARCHAR",
        "category": "BIGINT",
        "input_title": "VARCHAR",
        "title_slug": "VARCHAR",
        "selected_igdb_game_id": "BIGINT",
        "extract_date": "VARCHAR",
        "run_timestamp": "VARCHAR",
        "raw_file_path": "VARCHAR",
        "ingestion_status": "VARCHAR",
        "staging_status": "VARCHAR",
    },
    "release_dates": {
        "release_date_id": "BIGINT",
        "igdb_game_id": "BIGINT",
        "platform_id": "BIGINT",
        "region": "BIGINT",
        "release_date_timestamp": "BIGINT",
        "human": "VARCHAR",
        "input_title": "VARCHAR",
        "title_slug": "VARCHAR",
        "selected_igdb_game_id": "BIGINT",
        "extract_date": "VARCHAR",
        "run_timestamp": "VARCHAR",
        "raw_file_path": "VARCHAR",
        "ingestion_status": "VARCHAR",
        "staging_status": "VARCHAR",
    },
}


class IGDBReferenceStageError(RuntimeError):
    """Raised when an IGDB reference extract cannot be staged."""


@dataclass(frozen=True)
class RawIGDBReferenceExtract:
    """Raw IGDB reference extract selected for stage normalization."""

    raw_run_dir: Path
    metadata_file_path: Path | None
    input_title: str
    title_slug: str
    selected_igdb_game_id: int | None
    extract_date: str
    run_timestamp: str
    ingestion_status: str


@dataclass(frozen=True)
class IGDBEntityStageResult:
    """Summary of one staged IGDB entity output."""

    entity_name: str
    source_name: str
    raw_input_paths: tuple[Path, ...]
    staged_output_path: Path
    metadata_file_path: Path
    run_timestamp: str
    row_count: int
    status: str


@dataclass(frozen=True)
class IGDBReferenceStageResult:
    """Summary of one title-specific IGDB reference staging run."""

    input_title: str
    title_slug: str
    run_timestamp: str
    status: str
    entity_results: tuple[IGDBEntityStageResult, ...]


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise IGDBReferenceStageError(f"Raw metadata file does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise IGDBReferenceStageError(f"Raw metadata file is not valid JSON: {path}") from exc

    if not isinstance(payload, dict):
        raise IGDBReferenceStageError(f"Raw metadata file must contain a JSON object: {path}")
    return payload


def _read_json_list(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise IGDBReferenceStageError(f"Raw payload file does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise IGDBReferenceStageError(f"Raw payload file is not valid JSON: {path}") from exc

    if not isinstance(payload, list):
        raise IGDBReferenceStageError(f"Raw payload file must contain a JSON array: {path}")
    return [item for item in payload if isinstance(item, dict)]


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


def _base_row(extract: RawIGDBReferenceExtract, raw_file_path: Path) -> dict[str, Any]:
    return {
        "input_title": extract.input_title,
        "title_slug": extract.title_slug,
        "selected_igdb_game_id": extract.selected_igdb_game_id,
        "extract_date": extract.extract_date,
        "run_timestamp": extract.run_timestamp,
        "raw_file_path": str(raw_file_path),
        "ingestion_status": extract.ingestion_status,
        "staging_status": "success",
    }


def _raw_run_dir_from_path(raw_path: Path) -> Path:
    if raw_path.is_file():
        return raw_path.parent
    return raw_path


def build_raw_reference_extract(raw_path: Path) -> RawIGDBReferenceExtract:
    """Build raw extract metadata for a raw IGDB reference run directory or file."""

    raw_run_dir = _raw_run_dir_from_path(raw_path)
    metadata_file_path = raw_run_dir / "metadata.json"
    metadata = _read_json_object(metadata_file_path) if metadata_file_path.exists() else {}

    run_timestamp = (
        str(metadata.get("run_timestamp"))
        if metadata.get("run_timestamp")
        else _partition_value(raw_run_dir, "run_timestamp=")
    )
    if not run_timestamp:
        raise IGDBReferenceStageError("Could not determine run_timestamp from raw metadata or path.")

    title_slug = (
        str(metadata.get("title_slug"))
        if metadata.get("title_slug")
        else _partition_value(raw_run_dir, "title_slug=")
    )
    if not title_slug:
        raise IGDBReferenceStageError("Could not determine title_slug from raw metadata or path.")

    input_title = str(metadata.get("input_title") or title_slug.replace("-", " "))
    extract_date = (
        _partition_value(raw_run_dir, "extract_date=")
        or extract_date_from_run_timestamp(run_timestamp)
    )
    selected_igdb_game_id = _integer_or_none(metadata.get("selected_game_id"))
    ingestion_status = str(metadata.get("status", "unknown"))

    return RawIGDBReferenceExtract(
        raw_run_dir=raw_run_dir,
        metadata_file_path=metadata_file_path if metadata_file_path.exists() else None,
        input_title=input_title,
        title_slug=title_slug,
        selected_igdb_game_id=selected_igdb_game_id,
        extract_date=extract_date,
        run_timestamp=run_timestamp,
        ingestion_status=ingestion_status,
    )


def find_latest_successful_raw_reference_extracts(
    paths: ProjectPaths,
    *,
    title_slug: str | None = None,
) -> list[RawIGDBReferenceExtract]:
    """Find the latest successful raw IGDB reference extract for each selected title slug."""

    raw_root = paths.raw_data_dir / SOURCE_NAME / EXTRACTION_TYPE
    latest_by_title_slug: dict[str, RawIGDBReferenceExtract] = {}

    for metadata_file_path in raw_root.glob("title_slug=*/extract_date=*/run_timestamp=*/metadata.json"):
        metadata = _read_json_object(metadata_file_path)
        if metadata.get("status") != "success":
            continue

        candidate = build_raw_reference_extract(metadata_file_path.parent)
        if title_slug is not None and candidate.title_slug != title_slug:
            continue

        current = latest_by_title_slug.get(candidate.title_slug)
        if current is None or candidate.run_timestamp > current.run_timestamp:
            latest_by_title_slug[candidate.title_slug] = candidate

    if not latest_by_title_slug:
        title_filter = f" for title_slug={title_slug}" if title_slug is not None else ""
        raise IGDBReferenceStageError(
            f"No successful IGDB reference raw extract was found{title_filter}."
        )

    return [latest_by_title_slug[key] for key in sorted(latest_by_title_slug)]


def resolve_title_slug(title: str) -> str:
    """Resolve a CLI title value to the raw title slug convention."""

    return slugify_title(title)


def _payload_path(extract: RawIGDBReferenceExtract, entity_name: str) -> Path:
    file_name = "game_details.json" if entity_name == "games" else f"{entity_name}.json"
    return extract.raw_run_dir / file_name


def _read_entity_payload(
    extract: RawIGDBReferenceExtract,
    entity_name: str,
) -> tuple[Path, list[dict[str, Any]]]:
    path = _payload_path(extract, entity_name)
    if not path.exists():
        return path, []
    return path, _read_json_list(path)


def normalize_games(
    payload: list[dict[str, Any]],
    *,
    extract: RawIGDBReferenceExtract,
    raw_file_path: Path,
) -> list[dict[str, Any]]:
    """Normalize raw IGDB game details into stage-shaped rows."""

    rows: list[dict[str, Any]] = []
    for game in payload:
        rows.append(
            {
                "igdb_game_id": _integer_or_none(game.get("id")),
                "game_name": _string_or_none(game.get("name")),
                "slug": _string_or_none(game.get("slug")),
                "first_release_date": _integer_or_none(game.get("first_release_date")),
                "aggregated_rating": _float_or_none(game.get("aggregated_rating")),
                "aggregated_rating_count": _integer_or_none(game.get("aggregated_rating_count")),
                "category": _integer_or_none(game.get("category")),
                **_base_row(extract, raw_file_path),
            }
        )
    return rows


def normalize_involved_companies(
    payload: list[dict[str, Any]],
    *,
    extract: RawIGDBReferenceExtract,
    raw_file_path: Path,
) -> list[dict[str, Any]]:
    """Normalize raw IGDB involved companies into stage-shaped rows."""

    rows: list[dict[str, Any]] = []
    for involved_company in payload:
        rows.append(
            {
                "involved_company_id": _integer_or_none(involved_company.get("id")),
                "igdb_game_id": _integer_or_none(involved_company.get("game")),
                "company_id": _integer_or_none(involved_company.get("company")),
                "developer_flag": _bool_or_none(involved_company.get("developer")),
                "publisher_flag": _bool_or_none(involved_company.get("publisher")),
                "supporting_flag": _bool_or_none(involved_company.get("supporting")),
                "porting_flag": _bool_or_none(involved_company.get("porting")),
                **_base_row(extract, raw_file_path),
            }
        )
    return rows


def normalize_companies(
    payload: list[dict[str, Any]],
    *,
    extract: RawIGDBReferenceExtract,
    raw_file_path: Path,
) -> list[dict[str, Any]]:
    """Normalize raw IGDB companies into stage-shaped rows."""

    rows: list[dict[str, Any]] = []
    for company in payload:
        rows.append(
            {
                "company_id": _integer_or_none(company.get("id")),
                "company_name": _string_or_none(company.get("name")),
                "slug": _string_or_none(company.get("slug")),
                "country": _integer_or_none(company.get("country")),
                "start_date": _integer_or_none(company.get("start_date")),
                **_base_row(extract, raw_file_path),
            }
        )
    return rows


def normalize_genres(
    payload: list[dict[str, Any]],
    *,
    extract: RawIGDBReferenceExtract,
    raw_file_path: Path,
) -> list[dict[str, Any]]:
    """Normalize raw IGDB genres into stage-shaped rows."""

    rows: list[dict[str, Any]] = []
    for genre in payload:
        rows.append(
            {
                "genre_id": _integer_or_none(genre.get("id")),
                "genre_name": _string_or_none(genre.get("name")),
                "slug": _string_or_none(genre.get("slug")),
                **_base_row(extract, raw_file_path),
            }
        )
    return rows


def normalize_platforms(
    payload: list[dict[str, Any]],
    *,
    extract: RawIGDBReferenceExtract,
    raw_file_path: Path,
) -> list[dict[str, Any]]:
    """Normalize raw IGDB platforms into stage-shaped rows."""

    rows: list[dict[str, Any]] = []
    for platform in payload:
        rows.append(
            {
                "platform_id": _integer_or_none(platform.get("id")),
                "platform_name": _string_or_none(platform.get("name")),
                "slug": _string_or_none(platform.get("slug")),
                "category": _integer_or_none(platform.get("category")),
                **_base_row(extract, raw_file_path),
            }
        )
    return rows


def normalize_release_dates(
    payload: list[dict[str, Any]],
    *,
    extract: RawIGDBReferenceExtract,
    raw_file_path: Path,
) -> list[dict[str, Any]]:
    """Normalize raw IGDB release dates into stage-shaped rows."""

    rows: list[dict[str, Any]] = []
    for release_date in payload:
        rows.append(
            {
                "release_date_id": _integer_or_none(release_date.get("id")),
                "igdb_game_id": _integer_or_none(release_date.get("game")),
                "platform_id": _integer_or_none(release_date.get("platform")),
                "region": _integer_or_none(release_date.get("region")),
                "release_date_timestamp": _integer_or_none(release_date.get("date")),
                "human": _string_or_none(release_date.get("human")),
                **_base_row(extract, raw_file_path),
            }
        )
    return rows


def build_entity_stage_run_dir(
    stage_igdb_dir: Path,
    entity_name: str,
    title_slug: str,
    run_timestamp: str,
) -> Path:
    """Build the staged landing directory for one IGDB entity and title run."""

    extract_date = extract_date_from_run_timestamp(run_timestamp)
    return (
        stage_igdb_dir
        / EXTRACTION_TYPE
        / entity_name
        / f"title_slug={title_slug}"
        / f"extract_date={extract_date}"
        / f"run_timestamp={run_timestamp}"
    )


def write_entity_stage_parquet(
    *,
    entity_name: str,
    output_path: Path,
    rows: list[dict[str, Any]],
) -> None:
    """Write IGDB stage rows as Parquet using DuckDB."""

    columns = ENTITY_COLUMNS[entity_name]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    column_sql = ", ".join(f"{name} {data_type}" for name, data_type in columns.items())
    table_name = f"stage_igdb_{entity_name}"

    with duckdb.connect(database=":memory:") as connection:
        connection.execute(f"CREATE TABLE {table_name} ({column_sql})")
        if rows:
            placeholders = ", ".join("?" for _ in columns)
            values = [tuple(row.get(column) for column in columns) for row in rows]
            connection.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", values)
        connection.execute(
            "COPY {table_name} TO ? (FORMAT PARQUET)".format(table_name=table_name),
            [str(output_path)],
        )


def build_entity_stage_metadata(
    *,
    entity_name: str,
    raw_input_paths: list[Path],
    staged_output_path: Path,
    metadata_file_path: Path,
    run_timestamp: str,
    row_count: int,
    status: str,
) -> dict[str, Any]:
    """Build operational metadata for one staged IGDB entity output."""

    return {
        "entity_name": entity_name,
        "source_name": SOURCE_NAME,
        "transformation_type": f"stage_{EXTRACTION_TYPE}",
        "row_count": row_count,
        "raw_input_paths": [str(path) for path in raw_input_paths],
        "staged_output_path": str(staged_output_path),
        "metadata_file_path": str(metadata_file_path),
        "run_timestamp": run_timestamp,
        "status": status,
        "schema": [
            {"name": name, "type": data_type}
            for name, data_type in ENTITY_COLUMNS[entity_name].items()
        ],
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _normalize_entity(
    entity_name: str,
    payload: list[dict[str, Any]],
    *,
    extract: RawIGDBReferenceExtract,
    raw_file_path: Path,
) -> list[dict[str, Any]]:
    if entity_name == "games":
        return normalize_games(payload, extract=extract, raw_file_path=raw_file_path)
    if entity_name == "involved_companies":
        return normalize_involved_companies(payload, extract=extract, raw_file_path=raw_file_path)
    if entity_name == "companies":
        return normalize_companies(payload, extract=extract, raw_file_path=raw_file_path)
    if entity_name == "genres":
        return normalize_genres(payload, extract=extract, raw_file_path=raw_file_path)
    if entity_name == "platforms":
        return normalize_platforms(payload, extract=extract, raw_file_path=raw_file_path)
    if entity_name == "release_dates":
        return normalize_release_dates(payload, extract=extract, raw_file_path=raw_file_path)
    raise IGDBReferenceStageError(f"Unsupported IGDB entity: {entity_name}")


def stage_igdb_reference_extract(
    *,
    paths: ProjectPaths,
    extract: RawIGDBReferenceExtract,
) -> IGDBReferenceStageResult:
    """Normalize one raw IGDB reference extract into staged Parquet datasets."""

    entity_results: list[IGDBEntityStageResult] = []

    for entity_name in ENTITY_COLUMNS:
        raw_file_path, payload = _read_entity_payload(extract, entity_name)
        rows = _normalize_entity(
            entity_name,
            payload,
            extract=extract,
            raw_file_path=raw_file_path,
        )
        stage_run_dir = build_entity_stage_run_dir(
            paths.stage_data_dir / SOURCE_NAME,
            entity_name,
            extract.title_slug,
            extract.run_timestamp,
        )
        staged_output_path = stage_run_dir / f"{entity_name}.parquet"
        metadata_file_path = stage_run_dir / "metadata.json"

        write_entity_stage_parquet(
            entity_name=entity_name,
            output_path=staged_output_path,
            rows=rows,
        )
        raw_input_paths = [raw_file_path] if raw_file_path.exists() else []
        metadata = build_entity_stage_metadata(
            entity_name=entity_name,
            raw_input_paths=raw_input_paths,
            staged_output_path=staged_output_path,
            metadata_file_path=metadata_file_path,
            run_timestamp=extract.run_timestamp,
            row_count=len(rows),
            status="success",
        )
        write_json(metadata_file_path, metadata)
        entity_results.append(
            IGDBEntityStageResult(
                entity_name=entity_name,
                source_name=SOURCE_NAME,
                raw_input_paths=tuple(raw_input_paths),
                staged_output_path=staged_output_path,
                metadata_file_path=metadata_file_path,
                run_timestamp=extract.run_timestamp,
                row_count=len(rows),
                status="success",
            )
        )

    return IGDBReferenceStageResult(
        input_title=extract.input_title,
        title_slug=extract.title_slug,
        run_timestamp=extract.run_timestamp,
        status="success",
        entity_results=tuple(entity_results),
    )


def stage_igdb_reference(
    *,
    paths: ProjectPaths,
    title: str | None = None,
    raw_path: Path | None = None,
) -> list[IGDBReferenceStageResult]:
    """Stage latest successful raw IGDB reference extracts or a selected raw path."""

    if title and raw_path:
        raise IGDBReferenceStageError("Use either --title or --raw-path, not both.")

    extracts = (
        [build_raw_reference_extract(raw_path)]
        if raw_path
        else find_latest_successful_raw_reference_extracts(
            paths,
            title_slug=resolve_title_slug(title) if title else None,
        )
    )
    return [stage_igdb_reference_extract(paths=paths, extract=extract) for extract in extracts]
