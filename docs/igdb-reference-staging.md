# IGDB Reference Staging

This Phase 2 step normalizes raw IGDB reference payloads into local staged Parquet datasets. It prepares the repository for future dbt models and controlled Steam-to-IGDB mapping without changing the existing Steam pipeline or the final Steam-only mart.

## What It Does

The staging command reads successful raw IGDB reference runs under `data/raw/igdb/reference/`, normalizes each raw entity payload into a clean tabular shape, and writes one staged Parquet dataset per entity.

The command defaults to the latest successful raw run for each available title slug:

```powershell
game-market-analytics stage-igdb-reference
```

Stage one title:

```powershell
game-market-analytics stage-igdb-reference --title "Dota 2"
```

Stage a specific raw run directory or payload file:

```powershell
game-market-analytics stage-igdb-reference --raw-path data\raw\igdb\reference\title_slug=dota-2\extract_date=YYYY-MM-DD\run_timestamp=YYYYMMDDTHHMMSSZ
```

`--title` and `--raw-path` are mutually exclusive.

## Raw Input Convention

The command reads raw title-specific runs from:

```text
data/raw/igdb/reference/title_slug=<TITLE_SLUG>/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

Expected raw files are:

```text
game_details.json
involved_companies.json
companies.json
genres.json
platforms.json
release_dates.json
metadata.json
```

If an entity payload is missing, staging writes an empty Parquet file for that entity. Malformed JSON is treated as an error so bad raw inputs are not silently normalized.

## Staged Output Convention

Staged outputs land under:

```text
data/stage/igdb/reference/<ENTITY_NAME>/title_slug=<TITLE_SLUG>/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

Each entity directory contains:

```text
<entity_name>.parquet
metadata.json
```

Current staged entities:

- `games`
- `involved_companies`
- `companies`
- `genres`
- `platforms`
- `release_dates`

Each staged row preserves traceability fields such as:

- input title
- title slug
- selected IGDB game ID
- extract date
- raw run timestamp
- raw file path
- ingestion status
- staging status

## Metadata

Each staged entity output writes a simple `metadata.json` file with:

- entity name
- source name
- transformation type
- row count
- raw input paths
- staged output path
- metadata file path
- run timestamp
- status
- schema summary

## Deferred Work

This step does not implement:

- dbt sources or models for IGDB
- automatic Steam-to-IGDB entity resolution
- enrichment of `mart_steam__catalog_reputation_current`
- publisher, developer, genre, or platform marts
- dashboards, notebooks, orchestration, Docker, CI/CD, cloud setup, or fake data

Those steps are intentionally deferred until the staged IGDB datasets are inspected and the mapping strategy is explicit.
