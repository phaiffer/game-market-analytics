import json
from pathlib import Path

import duckdb

from game_market_analytics.ingestion.igdb.stage_reference import (
    IGDBReferenceStageError,
    RawIGDBReferenceExtract,
    build_entity_stage_metadata,
    build_entity_stage_run_dir,
    find_latest_successful_raw_reference_extracts,
    normalize_companies,
    normalize_games,
    normalize_genres,
    normalize_involved_companies,
    normalize_platforms,
    normalize_release_dates,
    resolve_title_slug,
    stage_igdb_reference,
)
from game_market_analytics.paths import ProjectPaths


def _write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _raw_reference_run_dir(root: Path, title_slug: str, run_timestamp: str) -> Path:
    extract_date = f"{run_timestamp[0:4]}-{run_timestamp[4:6]}-{run_timestamp[6:8]}"
    return (
        root
        / "data"
        / "raw"
        / "igdb"
        / "reference"
        / f"title_slug={title_slug}"
        / f"extract_date={extract_date}"
        / f"run_timestamp={run_timestamp}"
    )


def _write_raw_reference_run(
    root: Path,
    *,
    title: str,
    title_slug: str,
    run_timestamp: str,
    status: str = "success",
    include_all_payloads: bool = True,
) -> Path:
    run_dir = _raw_reference_run_dir(root, title_slug, run_timestamp)
    selected_game_id = 10
    _write_json(
        run_dir / "metadata.json",
        {
            "input_title": title,
            "title_slug": title_slug,
            "selected_game_id": selected_game_id,
            "status": status,
            "run_timestamp": run_timestamp,
        },
    )
    _write_json(run_dir / "games_search.json", [{"id": selected_game_id, "name": title}])
    if include_all_payloads:
        _write_json(
            run_dir / "game_details.json",
            [
                {
                    "id": selected_game_id,
                    "name": title,
                    "slug": title_slug,
                    "first_release_date": 1_700_000_000,
                    "aggregated_rating": "88.5",
                    "aggregated_rating_count": 12,
                    "category": 0,
                }
            ],
        )
        _write_json(
            run_dir / "involved_companies.json",
            [
                {
                    "id": 100,
                    "game": selected_game_id,
                    "company": 500,
                    "developer": True,
                    "publisher": False,
                    "supporting": False,
                    "porting": True,
                }
            ],
        )
        _write_json(
            run_dir / "companies.json",
            [{"id": 500, "name": "Valve", "slug": "valve", "country": 840, "start_date": 1}],
        )
        _write_json(run_dir / "genres.json", [{"id": 5, "name": "Shooter", "slug": "shooter"}])
        _write_json(
            run_dir / "platforms.json",
            [{"id": 6, "name": "PC", "slug": "win", "category": 4}],
        )
        _write_json(
            run_dir / "release_dates.json",
            [
                {
                    "id": 700,
                    "game": selected_game_id,
                    "platform": 6,
                    "region": 8,
                    "date": 1_700_000_000,
                    "human": "Nov 14, 2023",
                }
            ],
        )
    return run_dir


def _extract(tmp_path: Path) -> RawIGDBReferenceExtract:
    return RawIGDBReferenceExtract(
        raw_run_dir=tmp_path,
        metadata_file_path=tmp_path / "metadata.json",
        input_title="Counter-Strike 2",
        title_slug="counter-strike-2",
        selected_igdb_game_id=10,
        extract_date="2026-04-29",
        run_timestamp="20260429T120000Z",
        ingestion_status="success",
    )


def test_find_latest_successful_raw_reference_extracts_selects_latest_per_title(
    tmp_path: Path,
) -> None:
    _write_raw_reference_run(
        tmp_path,
        title="Dota 2",
        title_slug="dota-2",
        run_timestamp="20260428T120000Z",
    )
    newer_dota = _write_raw_reference_run(
        tmp_path,
        title="Dota 2",
        title_slug="dota-2",
        run_timestamp="20260429T120000Z",
    )
    _write_raw_reference_run(
        tmp_path,
        title="Dota 2",
        title_slug="dota-2",
        run_timestamp="20260430T120000Z",
        status="failed",
    )
    latest_cs2 = _write_raw_reference_run(
        tmp_path,
        title="Counter-Strike 2",
        title_slug="counter-strike-2",
        run_timestamp="20260429T130000Z",
    )

    extracts = find_latest_successful_raw_reference_extracts(ProjectPaths(project_root=tmp_path))

    assert [(extract.title_slug, extract.raw_run_dir) for extract in extracts] == [
        ("counter-strike-2", latest_cs2),
        ("dota-2", newer_dota),
    ]


def test_find_latest_successful_raw_reference_extracts_filters_title_slug(tmp_path: Path) -> None:
    _write_raw_reference_run(
        tmp_path,
        title="Dota 2",
        title_slug="dota-2",
        run_timestamp="20260429T120000Z",
    )
    latest_cs2 = _write_raw_reference_run(
        tmp_path,
        title="Counter-Strike 2",
        title_slug="counter-strike-2",
        run_timestamp="20260429T130000Z",
    )

    extracts = find_latest_successful_raw_reference_extracts(
        ProjectPaths(project_root=tmp_path),
        title_slug="counter-strike-2",
    )

    assert len(extracts) == 1
    assert extracts[0].raw_run_dir == latest_cs2


def test_resolve_title_slug_matches_raw_ingestion_convention() -> None:
    assert resolve_title_slug("Counter-Strike 2") == "counter-strike-2"
    assert resolve_title_slug("Pokémon: Legends / Arceus") == "pokemon-legends-arceus"


def test_normalize_games_flattens_stage_rows(tmp_path: Path) -> None:
    raw_file_path = tmp_path / "game_details.json"
    rows = normalize_games(
        [
            {
                "id": 10,
                "name": "Counter-Strike 2",
                "slug": "counter-strike-2",
                "first_release_date": 1_700_000_000,
                "aggregated_rating": "88.5",
                "aggregated_rating_count": 12,
                "category": 0,
            }
        ],
        extract=_extract(tmp_path),
        raw_file_path=raw_file_path,
    )

    assert rows == [
        {
            "igdb_game_id": 10,
            "game_name": "Counter-Strike 2",
            "slug": "counter-strike-2",
            "first_release_date": 1_700_000_000,
            "aggregated_rating": 88.5,
            "aggregated_rating_count": 12,
            "category": 0,
            "input_title": "Counter-Strike 2",
            "title_slug": "counter-strike-2",
            "selected_igdb_game_id": 10,
            "extract_date": "2026-04-29",
            "run_timestamp": "20260429T120000Z",
            "raw_file_path": str(raw_file_path),
            "ingestion_status": "success",
            "staging_status": "success",
        }
    ]


def test_normalize_involved_companies_flattens_stage_rows(tmp_path: Path) -> None:
    rows = normalize_involved_companies(
        [
            {
                "id": 100,
                "game": 10,
                "company": 500,
                "developer": True,
                "publisher": False,
                "supporting": True,
                "porting": False,
            }
        ],
        extract=_extract(tmp_path),
        raw_file_path=tmp_path / "involved_companies.json",
    )

    assert rows[0]["involved_company_id"] == 100
    assert rows[0]["igdb_game_id"] == 10
    assert rows[0]["company_id"] == 500
    assert rows[0]["developer_flag"] is True
    assert rows[0]["publisher_flag"] is False
    assert rows[0]["supporting_flag"] is True
    assert rows[0]["porting_flag"] is False


def test_normalize_companies_flattens_stage_rows(tmp_path: Path) -> None:
    rows = normalize_companies(
        [{"id": 500, "name": "Valve", "slug": "valve", "country": 840, "start_date": 1}],
        extract=_extract(tmp_path),
        raw_file_path=tmp_path / "companies.json",
    )

    assert rows[0]["company_id"] == 500
    assert rows[0]["company_name"] == "Valve"
    assert rows[0]["country"] == 840
    assert rows[0]["start_date"] == 1


def test_normalize_genres_flattens_stage_rows(tmp_path: Path) -> None:
    rows = normalize_genres(
        [{"id": 5, "name": "Shooter", "slug": "shooter"}],
        extract=_extract(tmp_path),
        raw_file_path=tmp_path / "genres.json",
    )

    assert rows[0]["genre_id"] == 5
    assert rows[0]["genre_name"] == "Shooter"
    assert rows[0]["slug"] == "shooter"


def test_normalize_platforms_flattens_stage_rows(tmp_path: Path) -> None:
    rows = normalize_platforms(
        [{"id": 6, "name": "PC", "slug": "win", "category": 4}],
        extract=_extract(tmp_path),
        raw_file_path=tmp_path / "platforms.json",
    )

    assert rows[0]["platform_id"] == 6
    assert rows[0]["platform_name"] == "PC"
    assert rows[0]["category"] == 4


def test_normalize_release_dates_flattens_stage_rows(tmp_path: Path) -> None:
    rows = normalize_release_dates(
        [
            {
                "id": 700,
                "game": 10,
                "platform": 6,
                "region": 8,
                "date": 1_700_000_000,
                "human": "Nov 14, 2023",
            }
        ],
        extract=_extract(tmp_path),
        raw_file_path=tmp_path / "release_dates.json",
    )

    assert rows[0]["release_date_id"] == 700
    assert rows[0]["igdb_game_id"] == 10
    assert rows[0]["platform_id"] == 6
    assert rows[0]["region"] == 8
    assert rows[0]["release_date_timestamp"] == 1_700_000_000
    assert rows[0]["human"] == "Nov 14, 2023"


def test_build_entity_stage_run_dir_uses_entity_and_title_partitions(tmp_path: Path) -> None:
    run_dir = build_entity_stage_run_dir(
        tmp_path / "data" / "stage" / "igdb",
        "games",
        "dota-2",
        "20260429T120000Z",
    )

    assert run_dir == (
        tmp_path
        / "data"
        / "stage"
        / "igdb"
        / "reference"
        / "games"
        / "title_slug=dota-2"
        / "extract_date=2026-04-29"
        / "run_timestamp=20260429T120000Z"
    )


def test_build_entity_stage_metadata_contains_schema_summary(tmp_path: Path) -> None:
    metadata = build_entity_stage_metadata(
        entity_name="games",
        raw_input_paths=[tmp_path / "game_details.json"],
        staged_output_path=tmp_path / "games.parquet",
        metadata_file_path=tmp_path / "metadata.json",
        run_timestamp="20260429T120000Z",
        row_count=1,
        status="success",
    )

    assert metadata["entity_name"] == "games"
    assert metadata["source_name"] == "igdb"
    assert metadata["transformation_type"] == "stage_reference"
    assert metadata["row_count"] == 1
    assert metadata["raw_input_paths"] == [str(tmp_path / "game_details.json")]
    assert {"name": "igdb_game_id", "type": "BIGINT"} in metadata["schema"]


def test_stage_igdb_reference_writes_entity_parquet_and_metadata(tmp_path: Path) -> None:
    paths = ProjectPaths(project_root=tmp_path)
    _write_raw_reference_run(
        tmp_path,
        title="Counter-Strike 2",
        title_slug="counter-strike-2",
        run_timestamp="20260429T120000Z",
    )

    results = stage_igdb_reference(paths=paths, title="Counter-Strike 2")

    assert len(results) == 1
    result = results[0]
    assert result.title_slug == "counter-strike-2"
    assert len(result.entity_results) == 6

    games_result = next(item for item in result.entity_results if item.entity_name == "games")
    assert games_result.row_count == 1
    assert games_result.staged_output_path.exists()
    assert games_result.metadata_file_path.exists()

    with duckdb.connect(database=":memory:") as connection:
        rows = connection.execute(
            "SELECT igdb_game_id, game_name, title_slug FROM read_parquet(?)",
            [str(games_result.staged_output_path)],
        ).fetchall()

    assert rows == [(10, "Counter-Strike 2", "counter-strike-2")]


def test_stage_igdb_reference_writes_empty_outputs_for_missing_payloads(tmp_path: Path) -> None:
    paths = ProjectPaths(project_root=tmp_path)
    _write_raw_reference_run(
        tmp_path,
        title="Dota 2",
        title_slug="dota-2",
        run_timestamp="20260429T120000Z",
        include_all_payloads=False,
    )

    results = stage_igdb_reference(paths=paths, title="Dota 2")

    assert len(results) == 1
    assert {item.entity_name: item.row_count for item in results[0].entity_results} == {
        "games": 0,
        "involved_companies": 0,
        "companies": 0,
        "genres": 0,
        "platforms": 0,
        "release_dates": 0,
    }


def test_stage_igdb_reference_rejects_malformed_payload(tmp_path: Path) -> None:
    paths = ProjectPaths(project_root=tmp_path)
    run_dir = _write_raw_reference_run(
        tmp_path,
        title="Dota 2",
        title_slug="dota-2",
        run_timestamp="20260429T120000Z",
    )
    (run_dir / "game_details.json").write_text("{bad json", encoding="utf-8")

    try:
        stage_igdb_reference(paths=paths, title="Dota 2")
    except IGDBReferenceStageError as exc:
        assert "not valid JSON" in str(exc)
    else:
        raise AssertionError("Expected IGDBReferenceStageError")
