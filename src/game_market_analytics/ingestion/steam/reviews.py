"""Raw landing flow for controlled Steam review extracts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from game_market_analytics.ingestion.steam.app_catalog import (
    SOURCE_NAME,
    extract_date_from_run_timestamp,
    utc_run_timestamp,
)
from game_market_analytics.ingestion.steam.review_client import (
    STEAM_REVIEWS_ENDPOINT_TEMPLATE,
    SteamReviewClient,
    SteamReviewClientError,
)
from game_market_analytics.paths import ProjectPaths


EXTRACTION_TYPE = "reviews"
DEFAULT_MAX_PAGES = 1
DEFAULT_LANGUAGE = "all"
DEFAULT_REVIEW_TYPE = "all"
DEFAULT_FILTER = "recent"
NUM_PER_PAGE = 100


class SteamReviewsInputError(ValueError):
    """Raised when requested Steam review inputs are invalid."""


@dataclass(frozen=True)
class SteamReviewsRunResult:
    """Summary of one app-specific Steam review ingestion run."""

    app_id: int
    status: str
    run_timestamp: str
    pages_fetched: int
    review_count: int
    output_files: tuple[Path, ...]
    metadata_file_path: Path
    pagination_stop_reason: str
    error_message: str | None = None


class ReviewPageClient(Protocol):
    endpoint_template: str

    def fetch_review_page(
        self,
        *,
        app_id: int,
        cursor: str,
        language: str,
        review_type: str,
        filter_value: str = DEFAULT_FILTER,
        num_per_page: int = NUM_PER_PAGE,
    ) -> dict[str, Any]:
        """Fetch one Steam review page."""


def parse_app_ids(app_id_values: list[str] | None, input_file: Path | None = None) -> list[int]:
    """Parse, validate, and deduplicate Steam app IDs."""

    raw_values: list[str] = []
    if app_id_values:
        raw_values.extend(app_id_values)

    if input_file:
        try:
            lines = input_file.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError as exc:
            raise SteamReviewsInputError(f"Input file does not exist: {input_file}") from exc
        raw_values.extend(
            line.strip()
            for line in lines
            if line.strip() and not line.strip().startswith("#")
        )

    if not raw_values:
        raise SteamReviewsInputError("Provide at least one --app-id or an --input-file.")

    app_ids: list[int] = []
    seen: set[int] = set()
    for raw_value in raw_values:
        try:
            app_id = int(str(raw_value).strip())
        except ValueError as exc:
            raise SteamReviewsInputError(f"Invalid Steam app ID: {raw_value}") from exc

        if app_id <= 0:
            raise SteamReviewsInputError(f"Steam app IDs must be positive integers: {raw_value}")

        if app_id not in seen:
            app_ids.append(app_id)
            seen.add(app_id)

    return app_ids


def build_reviews_run_dir(raw_steam_dir: Path, app_id: int, run_timestamp: str) -> Path:
    """Build the raw landing directory for one app review run."""

    extract_date = extract_date_from_run_timestamp(run_timestamp)
    return (
        raw_steam_dir
        / EXTRACTION_TYPE
        / f"app_id={app_id}"
        / f"extract_date={extract_date}"
        / f"run_timestamp={run_timestamp}"
    )


def review_count_from_payload(payload: dict[str, Any]) -> int:
    reviews = payload.get("reviews")
    return len(reviews) if isinstance(reviews, list) else 0


def next_cursor_from_payload(payload: dict[str, Any]) -> str | None:
    cursor = payload.get("cursor")
    return cursor if isinstance(cursor, str) and cursor else None


def should_continue_pagination(
    *,
    payload: dict[str, Any],
    current_cursor: str,
    seen_cursors: set[str],
) -> tuple[bool, str, str | None]:
    """Return pagination decision, stop reason, and next cursor."""

    if review_count_from_payload(payload) == 0:
        return False, "no_reviews_returned", None

    next_cursor = next_cursor_from_payload(payload)
    if not next_cursor:
        return False, "missing_next_cursor", None
    if next_cursor == current_cursor or next_cursor in seen_cursors:
        return False, "repeated_cursor", next_cursor

    return True, "next_cursor_available", next_cursor


def build_reviews_metadata(
    *,
    app_id: int,
    run_timestamp: str,
    status: str,
    pages_fetched: int,
    review_count: int,
    request_parameters: dict[str, Any],
    output_files: list[Path],
    pagination_stop_reason: str,
    metadata_file_path: Path,
    error_message: str | None = None,
) -> dict[str, Any]:
    """Build operational metadata for one Steam reviews run."""

    metadata = {
        "source_name": SOURCE_NAME,
        "extraction_type": EXTRACTION_TYPE,
        "app_id": app_id,
        "run_timestamp": run_timestamp,
        "status": status,
        "pages_fetched": pages_fetched,
        "review_count": review_count,
        "request_parameters": request_parameters,
        "output_files": [str(path) for path in output_files],
        "pagination_stop_reason": pagination_stop_reason,
        "metadata_file_path": str(metadata_file_path),
    }
    if error_message:
        metadata["error_message"] = error_message

    return metadata


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def ingest_reviews_for_app(
    *,
    paths: ProjectPaths,
    app_id: int,
    client: ReviewPageClient | None = None,
    max_pages: int = DEFAULT_MAX_PAGES,
    language: str = DEFAULT_LANGUAGE,
    review_type: str = DEFAULT_REVIEW_TYPE,
    filter_value: str = DEFAULT_FILTER,
    now: datetime | None = None,
) -> SteamReviewsRunResult:
    """Fetch and land raw Steam review pages for one app ID."""

    if max_pages <= 0:
        raise SteamReviewsInputError("--max-pages must be a positive integer.")

    run_timestamp = utc_run_timestamp(now)
    run_dir = build_reviews_run_dir(paths.raw_data_dir / SOURCE_NAME, app_id, run_timestamp)
    run_dir.mkdir(parents=True, exist_ok=True)
    metadata_file_path = run_dir / "metadata.json"

    review_client = client or SteamReviewClient()
    request_parameters = {
        "language": language,
        "review_type": review_type,
        "filter": filter_value,
        "purchase_type": "all",
        "num_per_page": NUM_PER_PAGE,
        "max_pages": max_pages,
        "endpoint": getattr(review_client, "endpoint_template", STEAM_REVIEWS_ENDPOINT_TEMPLATE),
    }

    output_files: list[Path] = []
    review_count = 0
    cursor = "*"
    seen_cursors = {cursor}
    stop_reason = "max_pages_reached"

    try:
        for page_number in range(1, max_pages + 1):
            payload = review_client.fetch_review_page(
                app_id=app_id,
                cursor=cursor,
                language=language,
                review_type=review_type,
                filter_value=filter_value,
                num_per_page=NUM_PER_PAGE,
            )

            output_file_path = run_dir / f"reviews_page_{page_number:04d}.json"
            write_json(output_file_path, payload)
            output_files.append(output_file_path)
            review_count += review_count_from_payload(payload)

            should_continue, page_stop_reason, next_cursor = should_continue_pagination(
                payload=payload,
                current_cursor=cursor,
                seen_cursors=seen_cursors,
            )
            stop_reason = page_stop_reason
            if not should_continue:
                break

            if page_number == max_pages:
                stop_reason = "max_pages_reached"
                break

            cursor = next_cursor or cursor
            seen_cursors.add(cursor)

    except SteamReviewClientError as exc:
        metadata = build_reviews_metadata(
            app_id=app_id,
            run_timestamp=run_timestamp,
            status="failed",
            pages_fetched=len(output_files),
            review_count=review_count,
            request_parameters=request_parameters,
            output_files=output_files,
            pagination_stop_reason="request_failed",
            metadata_file_path=metadata_file_path,
            error_message=str(exc),
        )
        write_json(metadata_file_path, metadata)
        return SteamReviewsRunResult(
            app_id=app_id,
            status="failed",
            run_timestamp=run_timestamp,
            pages_fetched=len(output_files),
            review_count=review_count,
            output_files=tuple(output_files),
            metadata_file_path=metadata_file_path,
            pagination_stop_reason="request_failed",
            error_message=str(exc),
        )

    metadata = build_reviews_metadata(
        app_id=app_id,
        run_timestamp=run_timestamp,
        status="success",
        pages_fetched=len(output_files),
        review_count=review_count,
        request_parameters=request_parameters,
        output_files=output_files,
        pagination_stop_reason=stop_reason,
        metadata_file_path=metadata_file_path,
    )
    write_json(metadata_file_path, metadata)

    return SteamReviewsRunResult(
        app_id=app_id,
        status="success",
        run_timestamp=run_timestamp,
        pages_fetched=len(output_files),
        review_count=review_count,
        output_files=tuple(output_files),
        metadata_file_path=metadata_file_path,
        pagination_stop_reason=stop_reason,
    )


def ingest_reviews_batch(
    *,
    paths: ProjectPaths,
    app_ids: list[int],
    client: ReviewPageClient | None = None,
    max_pages: int = DEFAULT_MAX_PAGES,
    language: str = DEFAULT_LANGUAGE,
    review_type: str = DEFAULT_REVIEW_TYPE,
    filter_value: str = DEFAULT_FILTER,
) -> list[SteamReviewsRunResult]:
    """Ingest Steam reviews for a controlled app ID batch."""

    return [
        ingest_reviews_for_app(
            paths=paths,
            app_id=app_id,
            client=client,
            max_pages=max_pages,
            language=language,
            review_type=review_type,
            filter_value=filter_value,
        )
        for app_id in app_ids
    ]
