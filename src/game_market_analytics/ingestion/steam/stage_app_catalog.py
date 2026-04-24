"""Normalize raw Steam app catalog extracts into a staged Parquet dataset."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from game_market_analytics.ingestion.steam.app_catalog import (
    EXTRACTION_TYPE,
    SOURCE_NAME,
    extract_date_from_run_timestamp,
    utc_run_timestamp,
)
from game_market_analytics.paths import ProjectPaths


STAGE_COLUMNS = {
    "source_system": "VARCHAR",
    "source_app_id": "BIGINT",
    "app_name": "VARCHAR",
    "item_type": "VARCHAR",
    "last_modified": "BIGINT",
    "price_change_number": "BIGINT",
    "extract_date": "VARCHAR",
    "run_timestamp": "VARCHAR",
    "raw_file_path": "VARCHAR",
    "ingestion_status": "VARCHAR",
}


class SteamAppCatalogStageError(RuntimeError):
    """Raised when a Steam app catalog extract cannot be staged."""


@dataclass(frozen=True)
class RawSteamAppCatalogExtract:
    """Raw extract selected for stage normalization."""

    raw_file_path: Path
    metadata_file_path: Path | None
    extract_date: str
    run_timestamp: str
    ingestion_status: str


@dataclass(frozen=True)
class SteamAppCatalogStageResult:
    """Summary of a Steam app catalog staging run."""

    source_name: str
    transformation_type: str
    raw_input_path: Path
    staged_output_path: Path
    metadata_file_path: Path
    run_timestamp: str
    row_count: int
    status: str


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SteamAppCatalogStageError(f"Raw file does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise SteamAppCatalogStageError(f"Raw file is not valid JSON: {path}") from exc

    if not isinstance(payload, dict):
        raise SteamAppCatalogStageError(f"Raw file must contain a JSON object: {path}")

    return payload


def _partition_value(path: Path, prefix: str) -> str | None:
    for part in path.parts:
        if part.startswith(prefix):
            return part.removeprefix(prefix)
    return None


def build_stage_run_dir(stage_steam_dir: Path, run_timestamp: str) -> Path:
    """Build the staged landing directory for a Steam app catalog run."""

    extract_date = extract_date_from_run_timestamp(run_timestamp)
    return (
        stage_steam_dir
        / EXTRACTION_TYPE
        / f"extract_date={extract_date}"
        / f"run_timestamp={run_timestamp}"
    )


def _raw_metadata_for_file(raw_file_path: Path) -> tuple[Path | None, dict[str, Any]]:
    metadata_file_path = raw_file_path.parent / "metadata.json"
    if not metadata_file_path.exists():
        return None, {}

    return metadata_file_path, _read_json(metadata_file_path)


def build_raw_extract(raw_file_path: Path) -> RawSteamAppCatalogExtract:
    """Build raw extract metadata for an explicit raw app catalog path."""

    metadata_file_path, metadata = _raw_metadata_for_file(raw_file_path)
    run_timestamp = (
        str(metadata.get("run_timestamp"))
        if metadata.get("run_timestamp")
        else _partition_value(raw_file_path, "run_timestamp=")
    )
    if not run_timestamp:
        raise SteamAppCatalogStageError(
            "Could not determine run_timestamp from raw metadata or path."
        )

    extract_date = (
        _partition_value(raw_file_path, "extract_date=")
        or extract_date_from_run_timestamp(run_timestamp)
    )
    ingestion_status = str(metadata.get("status", "unknown"))

    return RawSteamAppCatalogExtract(
        raw_file_path=raw_file_path,
        metadata_file_path=metadata_file_path,
        extract_date=extract_date,
        run_timestamp=run_timestamp,
        ingestion_status=ingestion_status,
    )


def find_latest_successful_raw_extract(paths: ProjectPaths) -> RawSteamAppCatalogExtract:
    """Find the latest successful raw Steam app catalog extract."""

    raw_root = paths.raw_data_dir / SOURCE_NAME / EXTRACTION_TYPE
    candidates: list[RawSteamAppCatalogExtract] = []

    for metadata_file_path in raw_root.glob("extract_date=*/run_timestamp=*/metadata.json"):
        metadata = _read_json(metadata_file_path)
        if metadata.get("status") != "success":
            continue

        raw_file_path = metadata.get("output_file_path")
        candidate_raw_file = Path(raw_file_path) if raw_file_path else metadata_file_path.parent / "app_catalog.json"
        if not candidate_raw_file.is_absolute():
            project_relative_path = paths.project_root / candidate_raw_file
            candidate_raw_file = (
                project_relative_path
                if project_relative_path.exists()
                else metadata_file_path.parent / candidate_raw_file
            )
        if not candidate_raw_file.exists():
            continue

        candidates.append(build_raw_extract(candidate_raw_file))

    if not candidates:
        raise SteamAppCatalogStageError("No successful Steam app catalog raw extract was found.")

    return max(candidates, key=lambda candidate: candidate.run_timestamp)


def _iter_app_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    pages = payload.get("pages")
    if isinstance(pages, list):
        records: list[dict[str, Any]] = []
        for page in pages:
            if not isinstance(page, dict):
                continue
            response = page.get("response")
            if not isinstance(response, dict):
                continue
            apps = response.get("apps")
            if isinstance(apps, list):
                records.extend(app for app in apps if isinstance(app, dict))
        return records

    response = payload.get("response")
    if isinstance(response, dict) and isinstance(response.get("apps"), list):
        return [app for app in response["apps"] if isinstance(app, dict)]

    applist = payload.get("applist")
    if isinstance(applist, dict) and isinstance(applist.get("apps"), list):
        return [app for app in applist["apps"] if isinstance(app, dict)]

    return []


def _integer_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def normalize_app_catalog_records(
    payload: dict[str, Any],
    *,
    extract: RawSteamAppCatalogExtract,
) -> list[dict[str, Any]]:
    """Normalize raw Steam app records into stage-shaped rows."""

    rows: list[dict[str, Any]] = []
    for app in _iter_app_records(payload):
        rows.append(
            {
                "source_system": SOURCE_NAME,
                "source_app_id": _integer_or_none(app.get("appid")),
                "app_name": _string_or_none(app.get("name")),
                "item_type": _string_or_none(app.get("type") or app.get("item_type")),
                "last_modified": _integer_or_none(app.get("last_modified")),
                "price_change_number": _integer_or_none(app.get("price_change_number")),
                "extract_date": extract.extract_date,
                "run_timestamp": extract.run_timestamp,
                "raw_file_path": str(extract.raw_file_path),
                "ingestion_status": extract.ingestion_status,
            }
        )

    return rows


def write_stage_parquet(output_path: Path, rows: list[dict[str, Any]]) -> None:
    """Write stage rows as Parquet using DuckDB."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    column_sql = ", ".join(f"{name} {data_type}" for name, data_type in STAGE_COLUMNS.items())

    with duckdb.connect(database=":memory:") as connection:
        connection.execute(f"CREATE TABLE stage_app_catalog ({column_sql})")
        if rows:
            placeholders = ", ".join("?" for _ in STAGE_COLUMNS)
            values = [tuple(row.get(column) for column in STAGE_COLUMNS) for row in rows]
            connection.executemany(
                f"INSERT INTO stage_app_catalog VALUES ({placeholders})",
                values,
            )
        connection.execute(
            "COPY stage_app_catalog TO ? (FORMAT PARQUET)",
            [str(output_path)],
        )


def build_stage_metadata(
    *,
    raw_input_path: Path,
    staged_output_path: Path,
    metadata_file_path: Path,
    run_timestamp: str,
    row_count: int,
    status: str,
) -> dict[str, Any]:
    """Build operational metadata for stage normalization."""

    return {
        "source_name": SOURCE_NAME,
        "transformation_type": f"stage_{EXTRACTION_TYPE}",
        "raw_input_path": str(raw_input_path),
        "staged_output_path": str(staged_output_path),
        "metadata_file_path": str(metadata_file_path),
        "run_timestamp": run_timestamp,
        "row_count": row_count,
        "schema": [{"name": name, "type": data_type} for name, data_type in STAGE_COLUMNS.items()],
        "status": status,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def stage_steam_app_catalog(
    *,
    paths: ProjectPaths,
    raw_file_path: Path | None = None,
    stage_run_timestamp: str | None = None,
) -> SteamAppCatalogStageResult:
    """Normalize a raw Steam app catalog extract into stage Parquet."""

    extract = (
        build_raw_extract(raw_file_path)
        if raw_file_path
        else find_latest_successful_raw_extract(paths)
    )
    payload = _read_json(extract.raw_file_path)
    rows = normalize_app_catalog_records(payload, extract=extract)

    run_timestamp = stage_run_timestamp or extract.run_timestamp or utc_run_timestamp()
    stage_run_dir = build_stage_run_dir(paths.stage_data_dir / SOURCE_NAME, run_timestamp)
    staged_output_path = stage_run_dir / "app_catalog.parquet"
    metadata_file_path = stage_run_dir / "metadata.json"

    write_stage_parquet(staged_output_path, rows)
    metadata = build_stage_metadata(
        raw_input_path=extract.raw_file_path,
        staged_output_path=staged_output_path,
        metadata_file_path=metadata_file_path,
        run_timestamp=run_timestamp,
        row_count=len(rows),
        status="success",
    )
    write_json(metadata_file_path, metadata)

    return SteamAppCatalogStageResult(
        source_name=SOURCE_NAME,
        transformation_type=f"stage_{EXTRACTION_TYPE}",
        raw_input_path=extract.raw_file_path,
        staged_output_path=staged_output_path,
        metadata_file_path=metadata_file_path,
        run_timestamp=run_timestamp,
        row_count=len(rows),
        status="success",
    )
