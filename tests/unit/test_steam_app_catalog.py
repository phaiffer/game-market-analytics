from datetime import UTC, datetime
import json
from pathlib import Path

from game_market_analytics.ingestion.steam.app_catalog import (
    build_app_catalog_run_dir,
    build_metadata,
    extract_app_count,
    ingest_steam_app_catalog,
    utc_run_timestamp,
)
from game_market_analytics.ingestion.steam.client import SteamClientError
from game_market_analytics.paths import ProjectPaths


class StubSteamClient:
    app_list_endpoint = "https://example.test/steam/apps"

    def fetch_app_list(self) -> dict:
        return {
            "pages": [
                {
                    "response": {
                        "apps": [
                            {"appid": 10, "name": "Counter-Strike"},
                            {"appid": 20, "name": "Team Fortress Classic"},
                        ]
                    }
                }
            ]
        }


class FailingSteamClient:
    def fetch_app_list(self) -> dict:
        raise SteamClientError("test failure")


def test_build_app_catalog_run_dir_uses_partitioned_convention(tmp_path: Path) -> None:
    run_dir = build_app_catalog_run_dir(tmp_path / "data" / "raw" / "steam", "20260424T153000Z")

    assert run_dir == (
        tmp_path
        / "data"
        / "raw"
        / "steam"
        / "app_catalog"
        / "extract_date=2026-04-24"
        / "run_timestamp=20260424T153000Z"
    )


def test_extract_app_count_reads_steam_payload_shape() -> None:
    payload = {"response": {"apps": [{"appid": 1}, {"appid": 2}, {"appid": 3}]}}
    paged_payload = {
        "pages": [
            {"response": {"apps": [{"appid": 1}, {"appid": 2}]}},
            {"response": {"apps": [{"appid": 3}]}},
        ]
    }

    assert extract_app_count(payload) == 3
    assert extract_app_count(paged_payload) == 3
    assert extract_app_count({"applist": {"apps": [{"appid": 1}]}}) == 1
    assert extract_app_count({"applist": {"apps": "unexpected"}}) == 0
    assert extract_app_count({}) == 0


def test_build_metadata_contains_operational_fields(tmp_path: Path) -> None:
    output_path = tmp_path / "app_catalog.json"

    metadata = build_metadata(
        run_timestamp="20260424T153000Z",
        endpoint="https://example.test/apps",
        status="success",
        app_count=2,
        output_file_path=output_path,
    )

    assert metadata["source_name"] == "steam"
    assert metadata["extraction_type"] == "app_catalog"
    assert metadata["run_timestamp"] == "20260424T153000Z"
    assert metadata["endpoint"] == "https://example.test/apps"
    assert metadata["status"] == "success"
    assert metadata["item_count"] == 2
    assert metadata["output_file_path"] == str(output_path)


def test_ingest_steam_app_catalog_writes_payload_and_metadata(tmp_path: Path) -> None:
    paths = ProjectPaths(project_root=tmp_path)
    now = datetime(2026, 4, 24, 15, 30, 0, tzinfo=UTC)

    result = ingest_steam_app_catalog(paths=paths, client=StubSteamClient(), now=now)

    assert result.status == "success"
    assert result.app_count == 2
    assert result.output_file_path.exists()
    assert result.metadata_file_path.exists()
    assert "extract_date=2026-04-24" in str(result.output_file_path)
    assert "run_timestamp=20260424T153000Z" in str(result.output_file_path)

    payload = json.loads(result.output_file_path.read_text(encoding="utf-8"))
    metadata = json.loads(result.metadata_file_path.read_text(encoding="utf-8"))

    assert extract_app_count(payload) == 2
    assert metadata["item_count"] == 2
    assert metadata["metadata_file_path"] == str(result.metadata_file_path)


def test_ingest_steam_app_catalog_writes_failure_metadata(tmp_path: Path) -> None:
    paths = ProjectPaths(project_root=tmp_path)
    now = datetime(2026, 4, 24, 15, 30, 0, tzinfo=UTC)

    try:
        ingest_steam_app_catalog(paths=paths, client=FailingSteamClient(), now=now)
    except SteamClientError:
        pass
    else:
        raise AssertionError("Expected SteamClientError")

    metadata_path = (
        tmp_path
        / "data"
        / "raw"
        / "steam"
        / "app_catalog"
        / "extract_date=2026-04-24"
        / "run_timestamp=20260424T153000Z"
        / "metadata.json"
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))

    assert metadata["status"] == "failed"
    assert metadata["item_count"] == 0
    assert metadata["error_message"] == "test failure"


def test_utc_run_timestamp_is_stable_for_injected_datetime() -> None:
    now = datetime(2026, 4, 24, 15, 30, 0, tzinfo=UTC)

    assert utc_run_timestamp(now) == "20260424T153000Z"
