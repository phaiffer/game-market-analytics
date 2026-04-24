from datetime import UTC, datetime
import json
from pathlib import Path

from game_market_analytics.ingestion.steam.review_client import SteamReviewClientError
from game_market_analytics.ingestion.steam.reviews import (
    SteamReviewsInputError,
    build_reviews_metadata,
    build_reviews_run_dir,
    ingest_reviews_batch,
    ingest_reviews_for_app,
    parse_app_ids,
    should_continue_pagination,
)
from game_market_analytics.paths import ProjectPaths


class PagedReviewClient:
    endpoint_template = "https://example.test/appreviews/{app_id}"

    def __init__(self, pages: list[dict]) -> None:
        self.pages = pages
        self.calls = []

    def fetch_review_page(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        return self.pages[len(self.calls) - 1]


class AppAwareReviewClient:
    endpoint_template = "https://example.test/appreviews/{app_id}"

    def fetch_review_page(self, **kwargs) -> dict:
        app_id = kwargs["app_id"]
        if app_id == 570:
            raise SteamReviewClientError("test failure")
        return {"reviews": [{"recommendationid": "1"}], "cursor": "next"}


def test_parse_app_ids_deduplicates_repeated_values(tmp_path: Path) -> None:
    input_file = tmp_path / "review_app_ids.txt"
    input_file.write_text("730\n# comment\n570\n730\n", encoding="utf-8")

    app_ids = parse_app_ids(["570", "10"], input_file=input_file)

    assert app_ids == [570, 10, 730]


def test_parse_app_ids_rejects_invalid_values() -> None:
    try:
        parse_app_ids(["0"], input_file=None)
    except SteamReviewsInputError as exc:
        assert "positive integers" in str(exc)
    else:
        raise AssertionError("Expected SteamReviewsInputError")


def test_build_reviews_run_dir_uses_app_partition(tmp_path: Path) -> None:
    run_dir = build_reviews_run_dir(tmp_path / "data" / "raw" / "steam", 570, "20260424T153000Z")

    assert run_dir == (
        tmp_path
        / "data"
        / "raw"
        / "steam"
        / "reviews"
        / "app_id=570"
        / "extract_date=2026-04-24"
        / "run_timestamp=20260424T153000Z"
    )


def test_build_reviews_metadata_contains_operational_fields(tmp_path: Path) -> None:
    metadata = build_reviews_metadata(
        app_id=570,
        run_timestamp="20260424T153000Z",
        status="success",
        pages_fetched=1,
        review_count=2,
        request_parameters={"language": "all", "max_pages": 1},
        output_files=[tmp_path / "reviews_page_0001.json"],
        pagination_stop_reason="max_pages_reached",
        metadata_file_path=tmp_path / "metadata.json",
    )

    assert metadata["source_name"] == "steam"
    assert metadata["extraction_type"] == "reviews"
    assert metadata["app_id"] == 570
    assert metadata["pages_fetched"] == 1
    assert metadata["review_count"] == 2
    assert metadata["pagination_stop_reason"] == "max_pages_reached"
    assert metadata["output_files"] == [str(tmp_path / "reviews_page_0001.json")]


def test_should_continue_pagination_stop_reasons() -> None:
    assert should_continue_pagination(
        payload={"reviews": []},
        current_cursor="*",
        seen_cursors={"*"},
    ) == (False, "no_reviews_returned", None)
    assert should_continue_pagination(
        payload={"reviews": [{"id": 1}]},
        current_cursor="*",
        seen_cursors={"*"},
    ) == (False, "missing_next_cursor", None)
    assert should_continue_pagination(
        payload={"reviews": [{"id": 1}], "cursor": "*"},
        current_cursor="*",
        seen_cursors={"*"},
    ) == (False, "repeated_cursor", "*")
    assert should_continue_pagination(
        payload={"reviews": [{"id": 1}], "cursor": "next"},
        current_cursor="*",
        seen_cursors={"*"},
    ) == (True, "next_cursor_available", "next")


def test_ingest_reviews_for_app_writes_pages_and_metadata(tmp_path: Path) -> None:
    client = PagedReviewClient(
        [
            {"reviews": [{"recommendationid": "1"}], "cursor": "cursor-2"},
            {"reviews": [], "cursor": "cursor-3"},
        ]
    )
    now = datetime(2026, 4, 24, 15, 30, 0, tzinfo=UTC)

    result = ingest_reviews_for_app(
        paths=ProjectPaths(project_root=tmp_path),
        app_id=570,
        client=client,
        max_pages=2,
        now=now,
    )

    assert result.status == "success"
    assert result.pages_fetched == 2
    assert result.review_count == 1
    assert result.pagination_stop_reason == "no_reviews_returned"
    assert len(result.output_files) == 2
    assert result.output_files[0].name == "reviews_page_0001.json"
    assert result.metadata_file_path.exists()

    metadata = json.loads(result.metadata_file_path.read_text(encoding="utf-8"))
    assert metadata["review_count"] == 1
    assert metadata["request_parameters"]["max_pages"] == 2


def test_ingest_reviews_for_app_writes_failure_metadata(tmp_path: Path) -> None:
    result = ingest_reviews_for_app(
        paths=ProjectPaths(project_root=tmp_path),
        app_id=570,
        client=AppAwareReviewClient(),
        max_pages=1,
        now=datetime(2026, 4, 24, 15, 30, 0, tzinfo=UTC),
    )

    assert result.status == "failed"
    assert result.pages_fetched == 0
    metadata = json.loads(result.metadata_file_path.read_text(encoding="utf-8"))
    assert metadata["status"] == "failed"
    assert metadata["pagination_stop_reason"] == "request_failed"
    assert metadata["error_message"] == "test failure"


def test_ingest_reviews_batch_continues_after_app_failure(tmp_path: Path) -> None:
    results = ingest_reviews_batch(
        paths=ProjectPaths(project_root=tmp_path),
        app_ids=[570, 730],
        client=AppAwareReviewClient(),
        max_pages=1,
    )

    assert [result.app_id for result in results] == [570, 730]
    assert [result.status for result in results] == ["failed", "success"]
