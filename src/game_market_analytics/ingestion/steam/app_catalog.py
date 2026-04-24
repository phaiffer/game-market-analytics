"""Raw landing flow for the Steam app catalog."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from game_market_analytics.ingestion.steam.client import (
    STEAM_APP_LIST_ENDPOINT,
    SteamClient,
    SteamClientError,
)
from game_market_analytics.paths import ProjectPaths


EXTRACTION_TYPE = "app_catalog"
SOURCE_NAME = "steam"


@dataclass(frozen=True)
class SteamAppCatalogRunResult:
    """Summary of a raw Steam app catalog ingestion run."""

    source_name: str
    extraction_type: str
    run_timestamp: str
    endpoint: str
    status: str
    app_count: int
    output_file_path: Path
    metadata_file_path: Path


class AppCatalogClient(Protocol):
    app_list_endpoint: str

    def fetch_app_list(self) -> dict[str, Any]:
        """Fetch Steam app catalog data."""


def utc_run_timestamp(now: datetime | None = None) -> str:
    """Return a compact UTC timestamp suitable for partition paths."""

    timestamp = now or datetime.now(UTC)
    return timestamp.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def extract_date_from_run_timestamp(run_timestamp: str) -> str:
    """Return YYYY-MM-DD from a run timestamp formatted as YYYYMMDDTHHMMSSZ."""

    return f"{run_timestamp[0:4]}-{run_timestamp[4:6]}-{run_timestamp[6:8]}"


def build_app_catalog_run_dir(raw_steam_dir: Path, run_timestamp: str) -> Path:
    """Build the raw landing directory for a Steam app catalog run."""

    extract_date = extract_date_from_run_timestamp(run_timestamp)
    return (
        raw_steam_dir
        / EXTRACTION_TYPE
        / f"extract_date={extract_date}"
        / f"run_timestamp={run_timestamp}"
    )


def extract_app_count(payload: dict[str, Any]) -> int:
    """Extract app count from a raw Steam app catalog payload."""

    pages = payload.get("pages")
    if isinstance(pages, list):
        total = 0
        for page in pages:
            if isinstance(page, dict):
                apps = page.get("response", {}).get("apps", [])
                if isinstance(apps, list):
                    total += len(apps)
        return total

    response = payload.get("response")
    if isinstance(response, dict):
        apps = response.get("apps", [])
        if isinstance(apps, list):
            return len(apps)

    applist = payload.get("applist")
    if isinstance(applist, dict):
        apps = applist.get("apps", [])
        if isinstance(apps, list):
            return len(apps)

    return 0


def build_metadata(
    *,
    run_timestamp: str,
    endpoint: str,
    status: str,
    app_count: int,
    output_file_path: Path,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Build simple operational metadata for a raw ingestion run."""

    metadata = {
        "source_name": SOURCE_NAME,
        "extraction_type": EXTRACTION_TYPE,
        "run_timestamp": run_timestamp,
        "endpoint": endpoint,
        "status": status,
        "item_count": app_count,
        "output_file_path": str(output_file_path),
    }
    if error_message:
        metadata["error_message"] = error_message

    return metadata


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write a JSON object with stable formatting."""

    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def ingest_steam_app_catalog(
    *,
    paths: ProjectPaths,
    client: AppCatalogClient | None = None,
    steam_api_key: str | None = None,
    now: datetime | None = None,
) -> SteamAppCatalogRunResult:
    """Fetch and land the raw Steam app catalog payload."""

    run_timestamp = utc_run_timestamp(now)
    steam_raw_dir = paths.raw_data_dir / SOURCE_NAME
    run_dir = build_app_catalog_run_dir(steam_raw_dir, run_timestamp)
    run_dir.mkdir(parents=True, exist_ok=True)
    output_file_path = run_dir / "app_catalog.json"
    metadata_file_path = run_dir / "metadata.json"

    try:
        steam_client = client or SteamClient(api_key=steam_api_key or "")
        payload = steam_client.fetch_app_list()
    except SteamClientError as exc:
        metadata = build_metadata(
            run_timestamp=run_timestamp,
            endpoint=STEAM_APP_LIST_ENDPOINT,
            status="failed",
            app_count=0,
            output_file_path=output_file_path,
            error_message=str(exc),
        )
        metadata["metadata_file_path"] = str(metadata_file_path)
        write_json(metadata_file_path, metadata)
        raise

    app_count = extract_app_count(payload)

    write_json(output_file_path, payload)
    metadata = build_metadata(
        run_timestamp=run_timestamp,
        endpoint=getattr(steam_client, "app_list_endpoint", STEAM_APP_LIST_ENDPOINT),
        status="success",
        app_count=app_count,
        output_file_path=output_file_path,
    )
    metadata["metadata_file_path"] = str(metadata_file_path)
    write_json(metadata_file_path, metadata)

    return SteamAppCatalogRunResult(
        source_name=SOURCE_NAME,
        extraction_type=EXTRACTION_TYPE,
        run_timestamp=run_timestamp,
        endpoint=metadata["endpoint"],
        status="success",
        app_count=app_count,
        output_file_path=output_file_path,
        metadata_file_path=metadata_file_path,
    )
