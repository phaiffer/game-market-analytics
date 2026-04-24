from pathlib import Path
import json

import duckdb

from game_market_analytics.ingestion.steam.stage_app_catalog import (
    RawSteamAppCatalogExtract,
    build_stage_metadata,
    build_stage_run_dir,
    find_latest_successful_raw_extract,
    normalize_app_catalog_records,
    stage_steam_app_catalog,
)
from game_market_analytics.paths import ProjectPaths


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _raw_run_dir(root: Path, run_timestamp: str) -> Path:
    extract_date = f"{run_timestamp[0:4]}-{run_timestamp[4:6]}-{run_timestamp[6:8]}"
    return (
        root
        / "data"
        / "raw"
        / "steam"
        / "app_catalog"
        / f"extract_date={extract_date}"
        / f"run_timestamp={run_timestamp}"
    )


def test_find_latest_successful_raw_extract(tmp_path: Path) -> None:
    older_dir = _raw_run_dir(tmp_path, "20260423T120000Z")
    newer_dir = _raw_run_dir(tmp_path, "20260424T120000Z")
    failed_dir = _raw_run_dir(tmp_path, "20260425T120000Z")

    for run_dir, status in (
        (older_dir, "success"),
        (newer_dir, "success"),
        (failed_dir, "failed"),
    ):
        raw_file = run_dir / "app_catalog.json"
        _write_json(raw_file, {"pages": []})
        _write_json(
            run_dir / "metadata.json",
            {
                "status": status,
                "run_timestamp": run_dir.name.removeprefix("run_timestamp="),
                "output_file_path": str(raw_file),
            },
        )

    selected = find_latest_successful_raw_extract(ProjectPaths(project_root=tmp_path))

    assert selected.raw_file_path == newer_dir / "app_catalog.json"
    assert selected.run_timestamp == "20260424T120000Z"
    assert selected.ingestion_status == "success"


def test_normalize_app_catalog_records_flattens_stage_rows(tmp_path: Path) -> None:
    extract = RawSteamAppCatalogExtract(
        raw_file_path=tmp_path / "app_catalog.json",
        metadata_file_path=None,
        extract_date="2026-04-24",
        run_timestamp="20260424T120000Z",
        ingestion_status="success",
    )
    payload = {
        "pages": [
            {
                "response": {
                    "apps": [
                        {
                            "appid": 10,
                            "name": "Counter-Strike",
                            "type": "game",
                            "last_modified": 123,
                            "price_change_number": 456,
                        }
                    ]
                }
            }
        ]
    }

    rows = normalize_app_catalog_records(payload, extract=extract)

    assert rows == [
        {
            "source_system": "steam",
            "source_app_id": 10,
            "app_name": "Counter-Strike",
            "item_type": "game",
            "last_modified": 123,
            "price_change_number": 456,
            "extract_date": "2026-04-24",
            "run_timestamp": "20260424T120000Z",
            "raw_file_path": str(tmp_path / "app_catalog.json"),
            "ingestion_status": "success",
        }
    ]


def test_normalize_app_catalog_records_handles_malformed_payload(tmp_path: Path) -> None:
    extract = RawSteamAppCatalogExtract(
        raw_file_path=tmp_path / "app_catalog.json",
        metadata_file_path=None,
        extract_date="2026-04-24",
        run_timestamp="20260424T120000Z",
        ingestion_status="success",
    )

    assert normalize_app_catalog_records({"pages": [{"response": {"apps": "bad"}}]}, extract=extract) == []
    assert normalize_app_catalog_records({}, extract=extract) == []


def test_build_stage_run_dir_uses_partitioned_convention(tmp_path: Path) -> None:
    run_dir = build_stage_run_dir(tmp_path / "data" / "stage" / "steam", "20260424T120000Z")

    assert run_dir == (
        tmp_path
        / "data"
        / "stage"
        / "steam"
        / "app_catalog"
        / "extract_date=2026-04-24"
        / "run_timestamp=20260424T120000Z"
    )


def test_build_stage_metadata_contains_schema_summary(tmp_path: Path) -> None:
    metadata = build_stage_metadata(
        raw_input_path=tmp_path / "raw.json",
        staged_output_path=tmp_path / "stage.parquet",
        metadata_file_path=tmp_path / "metadata.json",
        run_timestamp="20260424T120000Z",
        row_count=2,
        status="success",
    )

    assert metadata["source_name"] == "steam"
    assert metadata["transformation_type"] == "stage_app_catalog"
    assert metadata["row_count"] == 2
    assert metadata["status"] == "success"
    assert {"name": "source_app_id", "type": "BIGINT"} in metadata["schema"]


def test_stage_steam_app_catalog_writes_parquet_and_metadata(tmp_path: Path) -> None:
    paths = ProjectPaths(project_root=tmp_path)
    run_dir = _raw_run_dir(tmp_path, "20260424T120000Z")
    raw_file = run_dir / "app_catalog.json"
    _write_json(raw_file, {"response": {"apps": [{"appid": 10, "name": "Counter-Strike"}]}})
    _write_json(
        run_dir / "metadata.json",
        {
            "status": "success",
            "run_timestamp": "20260424T120000Z",
            "output_file_path": str(raw_file),
        },
    )

    result = stage_steam_app_catalog(paths=paths)

    assert result.row_count == 1
    assert result.staged_output_path.exists()
    assert result.metadata_file_path.exists()

    with duckdb.connect(database=":memory:") as connection:
        rows = connection.execute(
            "SELECT source_system, source_app_id, app_name FROM read_parquet(?)",
            [str(result.staged_output_path)],
        ).fetchall()

    assert rows == [("steam", 10, "Counter-Strike")]
