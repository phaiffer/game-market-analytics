# dbt IGDB Reference Models

This Phase 2 step makes staged IGDB reference Parquet datasets queryable through dbt and DuckDB. It adds explicit dbt sources, source-shaped staging models, and one small helper model for future controlled Steam-to-IGDB mapping.

## Prerequisites

Run the IGDB raw and stage flows first:

```powershell
game-market-analytics ingest-igdb-reference --title "Dota 2" --title "Counter-Strike 2"
game-market-analytics stage-igdb-reference
```

The dbt models expect staged Parquet files under:

```text
data/stage/igdb/reference/
```

## Sources

The `igdb_stage` source reads local staged Parquet files with dbt-duckdb external file reads:

- `igdb_stage.games`: `data/stage/igdb/reference/games/**/*.parquet`
- `igdb_stage.involved_companies`: `data/stage/igdb/reference/involved_companies/**/*.parquet`
- `igdb_stage.companies`: `data/stage/igdb/reference/companies/**/*.parquet`
- `igdb_stage.genres`: `data/stage/igdb/reference/genres/**/*.parquet`
- `igdb_stage.platforms`: `data/stage/igdb/reference/platforms/**/*.parquet`
- `igdb_stage.release_dates`: `data/stage/igdb/reference/release_dates/**/*.parquet`

## Staging Models

The staging models cast fields to stable types, standardize names, and preserve raw traceability columns such as `input_title`, `title_slug`, `extract_date`, `run_timestamp`, `raw_file_path`, `ingestion_status`, and `staging_status`.

- `stg_igdb__games`: one row per staged IGDB game reference row per title run.
- `stg_igdb__involved_companies`: one row per staged IGDB game-company relationship per title run.
- `stg_igdb__companies`: one row per staged IGDB company row per title run.
- `stg_igdb__genres`: one row per staged IGDB genre row per title run.
- `stg_igdb__platforms`: one row per staged IGDB platform row per title run.
- `stg_igdb__release_dates`: one row per staged IGDB release date row per title run.

The staging models intentionally preserve source repetition. For example, a platform or genre can appear for multiple curated titles.

## Helper Model

`int_igdb__games_latest_by_title` selects the latest valid staged IGDB game reference row per `title_slug`.

Grain:

- one row per `title_slug`

Purpose:

- provide a stable future input for controlled Steam-to-IGDB mapping
- keep title-level IGDB game candidates easy to inspect

## Run

Build the IGDB dbt layer:

```powershell
dbt build --project-dir dbt --profiles-dir dbt --select stg_igdb__games stg_igdb__involved_companies stg_igdb__companies stg_igdb__genres stg_igdb__platforms stg_igdb__release_dates int_igdb__games_latest_by_title
```

The command creates DuckDB views and runs lightweight tests for key IDs, traceability fields, statuses, and the one-row-per-title helper grain.

## Deferred Work

This step does not implement:

- automatic Steam-to-IGDB entity resolution
- enrichment of `mart_steam__catalog_reputation_current`
- IGDB marts or dimensions
- publisher, developer, genre, or platform analytics
- dashboards, notebooks, orchestration, Docker, CI/CD, or cloud setup

Those steps remain intentionally deferred until the mapping strategy is explicit.
