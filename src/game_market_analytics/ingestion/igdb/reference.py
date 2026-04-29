"""Raw landing flow for controlled IGDB title reference extracts."""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from game_market_analytics.ingestion.igdb.auth import request_igdb_access_token
from game_market_analytics.ingestion.igdb.client import IGDBClient, IGDBClientError
from game_market_analytics.ingestion.steam.app_catalog import (
    extract_date_from_run_timestamp,
    utc_run_timestamp,
)
from game_market_analytics.paths import ProjectPaths


SOURCE_NAME = "igdb"
EXTRACTION_TYPE = "reference"

GAME_SEARCH_FIELDS = (
    "id,name,slug,alternative_names.name,first_release_date,involved_companies,"
    "genres,platforms,release_dates"
)
GAME_DETAILS_FIELDS = (
    "id,name,slug,summary,storyline,first_release_date,total_rating,total_rating_count,"
    "rating,rating_count,aggregated_rating,aggregated_rating_count,involved_companies,"
    "genres,platforms,release_dates,parent_game,version_parent,collection,franchise,franchises"
)


class IGDBReferenceInputError(ValueError):
    """Raised when requested IGDB title inputs are invalid."""


class IGDBReferenceClient(Protocol):
    def post(self, endpoint: str, query: str) -> list[dict[str, Any]]:
        """POST a query to IGDB and return a raw JSON array."""


@dataclass(frozen=True)
class IGDBReferenceRunResult:
    """Summary of one title-specific IGDB reference ingestion run."""

    input_title: str
    title_slug: str
    status: str
    run_timestamp: str
    candidate_game_count: int
    selected_game_id: int | None
    files_written: tuple[Path, ...]
    metadata_file_path: Path
    warnings: tuple[str, ...] = ()
    error_message: str | None = None


def parse_titles(title_values: list[str] | None, input_file: Path | None = None) -> list[str]:
    """Parse and deduplicate title inputs while preserving first-seen order."""

    raw_values: list[str] = []
    if title_values:
        raw_values.extend(title_values)

    if input_file:
        try:
            lines = input_file.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError as exc:
            raise IGDBReferenceInputError(f"Input file does not exist: {input_file}") from exc
        raw_values.extend(line.strip() for line in lines if line.strip())

    if not raw_values:
        raise IGDBReferenceInputError("Provide at least one --title or an --input-file.")

    titles: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        title = str(raw_value).strip()
        if not title:
            continue
        dedupe_key = normalize_title_for_match(title)
        if dedupe_key not in seen:
            titles.append(title)
            seen.add(dedupe_key)

    if not titles:
        raise IGDBReferenceInputError("Provide at least one non-blank title.")

    return titles


def slugify_title(title: str) -> str:
    """Return a filesystem-friendly title slug."""

    ascii_title = (
        unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
    )
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_title.lower()).strip("-")
    return slug or "unknown-title"


def normalize_title_for_match(title: str) -> str:
    """Normalize a title for conservative exact matching."""

    ascii_title = (
        unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode("ascii")
    )
    return re.sub(r"[^a-z0-9]+", " ", ascii_title.lower()).strip()


def build_reference_run_dir(raw_igdb_dir: Path, title_slug: str, run_timestamp: str) -> Path:
    """Build the raw landing directory for one IGDB title reference run."""

    extract_date = extract_date_from_run_timestamp(run_timestamp)
    return (
        raw_igdb_dir
        / EXTRACTION_TYPE
        / f"title_slug={title_slug}"
        / f"extract_date={extract_date}"
        / f"run_timestamp={run_timestamp}"
    )


def write_json(path: Path, payload: list[dict[str, Any]] | dict[str, Any]) -> None:
    """Write a JSON payload with stable formatting."""

    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build_metadata(
    *,
    input_title: str,
    title_slug: str,
    run_timestamp: str,
    status: str,
    files_written: list[Path],
    candidate_game_count: int,
    selected_game_id: int | None,
    metadata_file_path: Path,
    warnings: list[str] | None = None,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Build operational metadata for one IGDB reference run."""

    metadata = {
        "source_name": SOURCE_NAME,
        "extraction_type": EXTRACTION_TYPE,
        "input_title": input_title,
        "title_slug": title_slug,
        "run_timestamp": run_timestamp,
        "status": status,
        "files_written": [str(path) for path in files_written],
        "candidate_game_count": candidate_game_count,
        "selected_game_id": selected_game_id,
        "warnings": warnings or [],
        "metadata_file_path": str(metadata_file_path),
    }
    if error_message:
        metadata["error_message"] = error_message

    return metadata


def select_clean_candidate(
    *,
    input_title: str,
    search_results: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, list[str]]:
    """Select one conservative candidate only when the title match is clean."""

    normalized_input = normalize_title_for_match(input_title)
    exact_matches = [
        item
        for item in search_results
        if normalize_title_for_match(str(item.get("name", ""))) == normalized_input
    ]
    if len(exact_matches) == 1:
        return exact_matches[0], []
    if len(exact_matches) > 1:
        return None, ["Multiple exact IGDB title matches found; no candidate was selected."]
    if len(search_results) == 1:
        return search_results[0], ["Only one IGDB search result was returned; selected cautiously."]
    if search_results:
        return None, ["No exact IGDB title match found; related entities were not fetched."]
    return None, ["No IGDB search results found."]


def _ids_from_game_details(game_details: list[dict[str, Any]], key: str) -> list[int]:
    if not game_details:
        return []
    value = game_details[0].get(key)
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, int)]


def _company_ids_from_involved_companies(
    involved_companies: list[dict[str, Any]],
) -> list[int]:
    company_ids: list[int] = []
    seen: set[int] = set()
    for item in involved_companies:
        company_id = item.get("company")
        if isinstance(company_id, int) and company_id not in seen:
            company_ids.append(company_id)
            seen.add(company_id)
    return company_ids


def _where_id_query(ids: list[int], fields: str) -> str:
    id_list = ",".join(str(item) for item in ids)
    return f"fields {fields}; where id = ({id_list}); limit {len(ids)};"


def _fetch_related_entities(
    *,
    client: IGDBReferenceClient,
    game_details: list[dict[str, Any]],
    run_dir: Path,
    files_written: list[Path],
) -> None:
    involved_company_ids = _ids_from_game_details(game_details, "involved_companies")
    if involved_company_ids:
        payload = client.post(
            "involved_companies",
            _where_id_query(
                involved_company_ids,
                "id,game,company,developer,publisher,porting,supporting",
            ),
        )
        path = run_dir / "involved_companies.json"
        write_json(path, payload)
        files_written.append(path)

        company_ids = _company_ids_from_involved_companies(payload)
        if company_ids:
            payload = client.post("companies", _where_id_query(company_ids, "id,name,slug,country"))
            path = run_dir / "companies.json"
            write_json(path, payload)
            files_written.append(path)

    for key, endpoint, fields, file_name in (
        ("genres", "genres", "id,name,slug", "genres.json"),
        ("platforms", "platforms", "id,name,slug,abbreviation,category", "platforms.json"),
        ("release_dates", "release_dates", "id,game,platform,date,region,human,y,m,date_format", "release_dates.json"),
    ):
        ids = _ids_from_game_details(game_details, key)
        if ids:
            payload = client.post(endpoint, _where_id_query(ids, fields))
            path = run_dir / file_name
            write_json(path, payload)
            files_written.append(path)


def ingest_reference_for_title(
    *,
    paths: ProjectPaths,
    title: str,
    client: IGDBReferenceClient,
    now: datetime | None = None,
) -> IGDBReferenceRunResult:
    """Fetch and land raw IGDB reference payloads for one curated title."""

    run_timestamp = utc_run_timestamp(now)
    title_slug = slugify_title(title)
    run_dir = build_reference_run_dir(paths.raw_data_dir / SOURCE_NAME, title_slug, run_timestamp)
    run_dir.mkdir(parents=True, exist_ok=True)
    metadata_file_path = run_dir / "metadata.json"

    files_written: list[Path] = []
    candidate_game_count = 0
    selected_game_id: int | None = None
    warnings: list[str] = []

    try:
        search_query = f'search "{title.replace(chr(34), " ")}"; fields {GAME_SEARCH_FIELDS}; limit 10;'
        search_results = client.post("games", search_query)
        candidate_game_count = len(search_results)
        path = run_dir / "games_search.json"
        write_json(path, search_results)
        files_written.append(path)

        selected_game, selection_warnings = select_clean_candidate(
            input_title=title,
            search_results=search_results,
        )
        warnings.extend(selection_warnings)
        if selected_game and isinstance(selected_game.get("id"), int):
            selected_game_id = selected_game["id"]
            game_details = client.post(
                "games",
                f"fields {GAME_DETAILS_FIELDS}; where id = {selected_game_id}; limit 1;",
            )
            path = run_dir / "game_details.json"
            write_json(path, game_details)
            files_written.append(path)
            _fetch_related_entities(
                client=client,
                game_details=game_details,
                run_dir=run_dir,
                files_written=files_written,
            )

    except IGDBClientError as exc:
        metadata = build_metadata(
            input_title=title,
            title_slug=title_slug,
            run_timestamp=run_timestamp,
            status="failed",
            files_written=files_written,
            candidate_game_count=candidate_game_count,
            selected_game_id=selected_game_id,
            metadata_file_path=metadata_file_path,
            warnings=warnings,
            error_message=str(exc),
        )
        write_json(metadata_file_path, metadata)
        return IGDBReferenceRunResult(
            input_title=title,
            title_slug=title_slug,
            status="failed",
            run_timestamp=run_timestamp,
            candidate_game_count=candidate_game_count,
            selected_game_id=selected_game_id,
            files_written=tuple(files_written),
            metadata_file_path=metadata_file_path,
            warnings=tuple(warnings),
            error_message=str(exc),
        )

    metadata = build_metadata(
        input_title=title,
        title_slug=title_slug,
        run_timestamp=run_timestamp,
        status="success",
        files_written=files_written,
        candidate_game_count=candidate_game_count,
        selected_game_id=selected_game_id,
        metadata_file_path=metadata_file_path,
        warnings=warnings,
    )
    write_json(metadata_file_path, metadata)
    return IGDBReferenceRunResult(
        input_title=title,
        title_slug=title_slug,
        status="success",
        run_timestamp=run_timestamp,
        candidate_game_count=candidate_game_count,
        selected_game_id=selected_game_id,
        files_written=tuple(files_written),
        metadata_file_path=metadata_file_path,
        warnings=tuple(warnings),
    )


def build_igdb_client(
    *,
    client_id: str | None,
    client_secret: str | None,
) -> IGDBClient:
    """Build an authenticated IGDB client from environment-backed credentials."""

    access_token = request_igdb_access_token(
        client_id=client_id,
        client_secret=client_secret,
    )
    return IGDBClient(client_id=client_id or "", access_token=access_token)


def ingest_reference_batch(
    *,
    paths: ProjectPaths,
    titles: list[str],
    client: IGDBReferenceClient | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> list[IGDBReferenceRunResult]:
    """Ingest IGDB reference payloads for a controlled title batch."""

    reference_client = client or build_igdb_client(
        client_id=client_id,
        client_secret=client_secret,
    )
    return [
        ingest_reference_for_title(
            paths=paths,
            title=title,
            client=reference_client,
        )
        for title in titles
    ]
