from datetime import UTC, datetime
import json
from pathlib import Path

from game_market_analytics.ingestion.igdb.client import IGDBClientError
from game_market_analytics.ingestion.igdb.reference import (
    IGDBReferenceInputError,
    build_metadata,
    build_reference_run_dir,
    ingest_reference_batch,
    ingest_reference_for_title,
    parse_titles,
    select_clean_candidate,
    slugify_title,
)
from game_market_analytics.paths import ProjectPaths


class ReferenceClient:
    def __init__(self) -> None:
        self.calls = []

    def post(self, endpoint: str, query: str) -> list[dict]:
        self.calls.append((endpoint, query))
        if endpoint == "games" and "search" in query:
            return [
                {
                    "id": 10,
                    "name": "Dota 2",
                    "involved_companies": [100],
                    "genres": [200],
                    "platforms": [300],
                    "release_dates": [400],
                }
            ]
        if endpoint == "games":
            return [
                {
                    "id": 10,
                    "name": "Dota 2",
                    "involved_companies": [100],
                    "genres": [200],
                    "platforms": [300],
                    "release_dates": [400],
                }
            ]
        if endpoint == "involved_companies":
            return [{"id": 100, "game": 10, "company": 500, "developer": True}]
        if endpoint == "companies":
            return [{"id": 500, "name": "Valve"}]
        if endpoint == "genres":
            return [{"id": 200, "name": "MOBA"}]
        if endpoint == "platforms":
            return [{"id": 300, "name": "PC"}]
        if endpoint == "release_dates":
            return [{"id": 400, "game": 10}]
        raise AssertionError(f"Unexpected endpoint: {endpoint}")


class AppAwareReferenceClient:
    def post(self, endpoint: str, query: str) -> list[dict]:
        if 'search "Broken Game"' in query:
            raise IGDBClientError("test failure")
        return [{"id": 1, "name": "Working Game"}]


def test_parse_titles_deduplicates_and_ignores_blank_lines(tmp_path: Path) -> None:
    input_file = tmp_path / "igdb_titles.txt"
    input_file.write_text("Counter-Strike 2\n\nDOTA 2\nDota 2\n", encoding="utf-8")

    titles = parse_titles(["Dota 2", "  Counter-Strike 2  "], input_file=input_file)

    assert titles == ["Dota 2", "Counter-Strike 2"]


def test_parse_titles_rejects_missing_values() -> None:
    try:
        parse_titles(None, input_file=None)
    except IGDBReferenceInputError as exc:
        assert "Provide at least one" in str(exc)
    else:
        raise AssertionError("Expected IGDBReferenceInputError")


def test_slugify_title_returns_path_safe_slug() -> None:
    assert slugify_title("Counter-Strike 2") == "counter-strike-2"
    assert slugify_title("Pokémon: Legends / Arceus") == "pokemon-legends-arceus"


def test_build_reference_run_dir_uses_title_slug_partition(tmp_path: Path) -> None:
    run_dir = build_reference_run_dir(
        tmp_path / "data" / "raw" / "igdb",
        "dota-2",
        "20260429T153000Z",
    )

    assert run_dir == (
        tmp_path
        / "data"
        / "raw"
        / "igdb"
        / "reference"
        / "title_slug=dota-2"
        / "extract_date=2026-04-29"
        / "run_timestamp=20260429T153000Z"
    )


def test_build_metadata_contains_operational_fields(tmp_path: Path) -> None:
    metadata = build_metadata(
        input_title="Dota 2",
        title_slug="dota-2",
        run_timestamp="20260429T153000Z",
        status="success",
        files_written=[tmp_path / "games_search.json"],
        candidate_game_count=1,
        selected_game_id=10,
        metadata_file_path=tmp_path / "metadata.json",
        warnings=["test warning"],
    )

    assert metadata["source_name"] == "igdb"
    assert metadata["extraction_type"] == "reference"
    assert metadata["input_title"] == "Dota 2"
    assert metadata["title_slug"] == "dota-2"
    assert metadata["candidate_game_count"] == 1
    assert metadata["selected_game_id"] == 10
    assert metadata["warnings"] == ["test warning"]


def test_select_clean_candidate_requires_conservative_match() -> None:
    candidate, warnings = select_clean_candidate(
        input_title="Dota 2",
        search_results=[{"id": 10, "name": "Dota 2"}],
    )
    assert candidate == {"id": 10, "name": "Dota 2"}
    assert warnings == []

    candidate, warnings = select_clean_candidate(
        input_title="Dota 2",
        search_results=[
            {"id": 10, "name": "Dota 2"},
            {"id": 11, "name": "Dota 2"},
        ],
    )
    assert candidate is None
    assert "Multiple exact" in warnings[0]


def test_ingest_reference_for_title_writes_search_related_files_and_metadata(
    tmp_path: Path,
) -> None:
    client = ReferenceClient()
    now = datetime(2026, 4, 29, 15, 30, 0, tzinfo=UTC)

    result = ingest_reference_for_title(
        paths=ProjectPaths(project_root=tmp_path),
        title="Dota 2",
        client=client,
        now=now,
    )

    assert result.status == "success"
    assert result.candidate_game_count == 1
    assert result.selected_game_id == 10
    assert result.metadata_file_path.exists()
    assert {path.name for path in result.files_written} == {
        "games_search.json",
        "game_details.json",
        "involved_companies.json",
        "companies.json",
        "genres.json",
        "platforms.json",
        "release_dates.json",
    }

    metadata = json.loads(result.metadata_file_path.read_text(encoding="utf-8"))
    assert metadata["status"] == "success"
    assert metadata["selected_game_id"] == 10
    assert metadata["candidate_game_count"] == 1
    assert len(metadata["files_written"]) == 7


def test_ingest_reference_for_title_writes_failure_metadata(tmp_path: Path) -> None:
    result = ingest_reference_for_title(
        paths=ProjectPaths(project_root=tmp_path),
        title="Broken Game",
        client=AppAwareReferenceClient(),
        now=datetime(2026, 4, 29, 15, 30, 0, tzinfo=UTC),
    )

    assert result.status == "failed"
    metadata = json.loads(result.metadata_file_path.read_text(encoding="utf-8"))
    assert metadata["status"] == "failed"
    assert metadata["error_message"] == "test failure"


def test_ingest_reference_batch_continues_after_title_failure(tmp_path: Path) -> None:
    results = ingest_reference_batch(
        paths=ProjectPaths(project_root=tmp_path),
        titles=["Broken Game", "Working Game"],
        client=AppAwareReferenceClient(),
    )

    assert [result.input_title for result in results] == ["Broken Game", "Working Game"]
    assert [result.status for result in results] == ["failed", "success"]
