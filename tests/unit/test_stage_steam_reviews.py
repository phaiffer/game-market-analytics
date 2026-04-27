from datetime import datetime
import json
from pathlib import Path

import duckdb

from game_market_analytics.ingestion.steam.stage_reviews import (
    RawSteamReviewsExtract,
    build_reviews_stage_metadata,
    build_reviews_stage_run_dir,
    find_latest_successful_raw_review_extracts,
    normalize_review_records,
    stage_steam_reviews,
)
from game_market_analytics.paths import ProjectPaths


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _raw_reviews_run_dir(root: Path, app_id: int, run_timestamp: str) -> Path:
    extract_date = f"{run_timestamp[0:4]}-{run_timestamp[4:6]}-{run_timestamp[6:8]}"
    return (
        root
        / "data"
        / "raw"
        / "steam"
        / "reviews"
        / f"app_id={app_id}"
        / f"extract_date={extract_date}"
        / f"run_timestamp={run_timestamp}"
    )


def _write_raw_review_run(
    root: Path,
    *,
    app_id: int,
    run_timestamp: str,
    status: str = "success",
    pages: list[dict] | None = None,
) -> Path:
    run_dir = _raw_reviews_run_dir(root, app_id, run_timestamp)
    page_paths = []
    for index, payload in enumerate(pages or [{"reviews": []}], start=1):
        page_path = run_dir / f"reviews_page_{index:04d}.json"
        _write_json(page_path, payload)
        page_paths.append(page_path)

    _write_json(
        run_dir / "metadata.json",
        {
            "app_id": app_id,
            "status": status,
            "run_timestamp": run_timestamp,
            "output_files": [str(path) for path in page_paths],
        },
    )
    return run_dir


def test_find_latest_successful_raw_review_extracts_selects_latest_per_app(tmp_path: Path) -> None:
    _write_raw_review_run(tmp_path, app_id=570, run_timestamp="20260423T120000Z")
    newer_570 = _write_raw_review_run(tmp_path, app_id=570, run_timestamp="20260424T120000Z")
    _write_raw_review_run(tmp_path, app_id=570, run_timestamp="20260425T120000Z", status="failed")
    latest_730 = _write_raw_review_run(tmp_path, app_id=730, run_timestamp="20260422T120000Z")

    extracts = find_latest_successful_raw_review_extracts(ProjectPaths(project_root=tmp_path))

    assert [(extract.app_id, extract.raw_run_dir) for extract in extracts] == [
        (570, newer_570),
        (730, latest_730),
    ]


def test_find_latest_successful_raw_review_extracts_filters_app_id(tmp_path: Path) -> None:
    _write_raw_review_run(tmp_path, app_id=570, run_timestamp="20260424T120000Z")
    latest_730 = _write_raw_review_run(tmp_path, app_id=730, run_timestamp="20260425T120000Z")

    extracts = find_latest_successful_raw_review_extracts(
        ProjectPaths(project_root=tmp_path),
        app_id=730,
    )

    assert len(extracts) == 1
    assert extracts[0].app_id == 730
    assert extracts[0].raw_run_dir == latest_730


def test_normalize_review_records_flattens_review_payload(tmp_path: Path) -> None:
    raw_file_path = tmp_path / "reviews_page_0001.json"
    extract = RawSteamReviewsExtract(
        app_id=570,
        raw_run_dir=tmp_path,
        page_file_paths=(raw_file_path,),
        metadata_file_path=None,
        extract_date="2026-04-24",
        run_timestamp="20260424T120000Z",
        ingestion_status="success",
    )
    payload = {
        "query_summary": {"review_score_desc": "Very Positive"},
        "reviews": [
            {
                "recommendationid": "123",
                "review": "Still good.",
                "language": "english",
                "voted_up": True,
                "votes_up": 4,
                "votes_funny": 1,
                "weighted_vote_score": "0.75",
                "steam_purchase": True,
                "received_for_free": False,
                "written_during_early_access": False,
                "timestamp_created": 1_700_000_000,
                "timestamp_updated": 1_700_000_100,
                "author": {
                    "steamid": "abc",
                    "num_games_owned": 20,
                    "num_reviews": 3,
                },
            }
        ],
    }

    rows = normalize_review_records(payload, extract=extract, raw_file_path=raw_file_path)

    assert len(rows) == 1
    assert rows[0]["source_system"] == "steam"
    assert rows[0]["source_app_id"] == 570
    assert rows[0]["review_id"] == "123"
    assert rows[0]["review_text"] == "Still good."
    assert rows[0]["review_score_desc"] == "Very Positive"
    assert rows[0]["weighted_vote_score"] == 0.75
    assert rows[0]["review_created_at"] == datetime(2023, 11, 14, 22, 13, 20)
    assert rows[0]["author_steamid"] == "abc"
    assert rows[0]["raw_file_path"] == str(raw_file_path)


def test_normalize_review_records_handles_malformed_payload(tmp_path: Path) -> None:
    extract = RawSteamReviewsExtract(
        app_id=570,
        raw_run_dir=tmp_path,
        page_file_paths=(tmp_path / "reviews_page_0001.json",),
        metadata_file_path=None,
        extract_date="2026-04-24",
        run_timestamp="20260424T120000Z",
        ingestion_status="success",
    )

    assert normalize_review_records({}, extract=extract, raw_file_path=tmp_path / "page.json") == []
    assert (
        normalize_review_records(
            {"reviews": "bad"},
            extract=extract,
            raw_file_path=tmp_path / "page.json",
        )
        == []
    )


def test_build_reviews_stage_run_dir_uses_app_partition(tmp_path: Path) -> None:
    run_dir = build_reviews_stage_run_dir(
        tmp_path / "data" / "stage" / "steam",
        570,
        "20260424T120000Z",
    )

    assert run_dir == (
        tmp_path
        / "data"
        / "stage"
        / "steam"
        / "reviews"
        / "app_id=570"
        / "extract_date=2026-04-24"
        / "run_timestamp=20260424T120000Z"
    )


def test_build_reviews_stage_metadata_contains_schema_summary(tmp_path: Path) -> None:
    raw_file_path = tmp_path / "reviews_page_0001.json"
    extract = RawSteamReviewsExtract(
        app_id=570,
        raw_run_dir=tmp_path,
        page_file_paths=(raw_file_path,),
        metadata_file_path=None,
        extract_date="2026-04-24",
        run_timestamp="20260424T120000Z",
        ingestion_status="success",
    )

    metadata = build_reviews_stage_metadata(
        extract=extract,
        staged_output_path=tmp_path / "reviews.parquet",
        metadata_file_path=tmp_path / "metadata.json",
        row_count=2,
        pages_processed=1,
        status="success",
    )

    assert metadata["source_name"] == "steam"
    assert metadata["transformation_type"] == "stage_reviews"
    assert metadata["app_id"] == 570
    assert metadata["raw_input_paths"] == [str(raw_file_path)]
    assert metadata["row_count"] == 2
    assert metadata["pages_processed"] == 1
    assert {"name": "review_id", "type": "VARCHAR"} in metadata["schema"]


def test_stage_steam_reviews_writes_parquet_and_metadata_for_paginated_run(tmp_path: Path) -> None:
    paths = ProjectPaths(project_root=tmp_path)
    _write_raw_review_run(
        tmp_path,
        app_id=570,
        run_timestamp="20260424T120000Z",
        pages=[
            {"reviews": [{"recommendationid": "1", "review": "one"}]},
            {"reviews": [{"recommendationid": "2", "review": "two"}]},
        ],
    )

    results = stage_steam_reviews(paths=paths, app_id=570)

    assert len(results) == 1
    result = results[0]
    assert result.row_count == 2
    assert result.pages_processed == 2
    assert result.staged_output_path.exists()
    assert result.metadata_file_path.exists()

    with duckdb.connect(database=":memory:") as connection:
        rows = connection.execute(
            "SELECT source_app_id, review_id, review_text FROM read_parquet(?) ORDER BY review_id",
            [str(result.staged_output_path)],
        ).fetchall()

    assert rows == [(570, "1", "one"), (570, "2", "two")]

    metadata = json.loads(result.metadata_file_path.read_text(encoding="utf-8"))
    assert metadata["row_count"] == 2
    assert metadata["pages_processed"] == 2
