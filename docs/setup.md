# Local Setup

This project is designed to run locally on Windows with a small Python, DuckDB, and dbt toolchain.

## Python Version

Use Python 3.11 or 3.12. The dbt dependency stack used by this project is not currently validated on Python 3.13+.

Check your version:

```powershell
python --version
```

## Virtual Environment

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If PowerShell blocks activation, use your organization-approved execution policy approach or activate from another shell.

## Install Dependencies

Install the package in editable mode with development dependencies:

```powershell
python -m pip install -e ".[dev]"
```

Or use the Makefile if `make` is available:

```powershell
make setup
```

## Environment Variables

Copy `.env.example` to `.env` for local use:

```powershell
Copy-Item .env.example .env
```

`STEAM_API_KEY` is used by the Steam app catalog ingestion flow. `IGDB_CLIENT_ID` and `IGDB_CLIENT_SECRET` are used by the controlled IGDB reference ingestion flow. The IsThereAnyDeal credential value remains a placeholder for a future source integration.

The default local DuckDB convention is:

```text
.local/game_market_analytics.duckdb
```

You can override it with `DUCKDB_PATH` in your shell or `.env` file later. The current CLI reads environment variables from the active shell and does not require a `.env` parser.

## Initialize Local Runtime Folders

Run:

```powershell
game-market-analytics init-local
```

This creates writable local folders such as `.local/` and confirms the main repository paths. It does not create source data or run ingestion.

## Validate the Repository

Run:

```powershell
game-market-analytics validate-project
```

This checks that the expected scaffold directories exist.

You can also print the active paths:

```powershell
game-market-analytics show-paths
```

Equivalent Makefile shortcuts:

```powershell
make validate
make show-paths
make test
```

## Run the Steam App Catalog Ingestion

The first real ingestion utility lands the raw Steam app catalog payload locally:

```powershell
game-market-analytics ingest-steam-app-catalog
```

Or:

```powershell
make ingest-steam-app-catalog
```

The current official Steam Web API app list endpoint requires a Steam Web API key. Add it to your active shell or local `.env` file:

```powershell
$env:STEAM_API_KEY = "your-key-here"
$env:STEAM_API_KEY_AUTH_LOCATION = "query"
```

The default and expected local authentication format sends the key as Steam's `key` query parameter. If you need to test header-based authentication, set:

```powershell
$env:STEAM_API_KEY_AUTH_LOCATION = "header"
```

Valid values are `query` and `header`. The header mode sends the key as `x-webapi-key`.

The command fetches `https://api.steampowered.com/IStoreService/GetAppList/v1/`, writes the raw JSON response, and writes run metadata next to the payload.

Successful runs land under:

```text
data/raw/steam/app_catalog/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

The directory contains:

```text
app_catalog.json
metadata.json
```

This command does not fetch Steam reviews, create dbt models, write to DuckDB, or normalize the payload.

## Run Controlled Steam Reviews Ingestion

Steam reviews are ingested only for explicit app IDs. This avoids accidental full-catalog crawling.

Repeated app IDs:

```powershell
game-market-analytics ingest-steam-reviews --app-id 570 --app-id 730 --max-pages 1
```

Input file:

```powershell
game-market-analytics ingest-steam-reviews --input-file .local\review_app_ids.txt
```

The input file should contain one positive integer app ID per line. Blank lines and lines starting with `#` are ignored.

Review raw files land under:

```text
data/raw/steam/reviews/app_id=<APP_ID>/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

Each app-specific run writes page-level JSON payloads and a `metadata.json` file. If one app fails, the command records failure metadata for that app and continues with the next app ID.

This command lands raw review payloads only. Use the staging command below to write review Parquet.

## Run Controlled IGDB Reference Ingestion

IGDB reference ingestion is title-driven and intentionally controlled. It searches IGDB for explicit curated game titles, writes the raw search response, and fetches related raw entity payloads only when a clean candidate can be selected.

Set Twitch/IGDB client credentials in your active shell or local `.env` file:

```powershell
$env:IGDB_CLIENT_ID = "your-client-id"
$env:IGDB_CLIENT_SECRET = "your-client-secret"
```

Repeated titles:

```powershell
game-market-analytics ingest-igdb-reference --title "Dota 2" --title "Counter-Strike 2"
```

Input file:

```powershell
game-market-analytics ingest-igdb-reference --input-file .local\igdb_titles.txt
```

The input file should contain one game title per line. Blank lines are ignored. Repeated titles are deduplicated before ingestion.

Raw files land under:

```text
data/raw/igdb/reference/title_slug=<TITLE_SLUG>/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

Each title-specific run writes `games_search.json`, `metadata.json`, and, when a clean candidate is selected, related files such as `game_details.json`, `involved_companies.json`, `companies.json`, `genres.json`, `platforms.json`, and `release_dates.json`.

This command lands raw IGDB payloads only. It does not stage IGDB data, create dbt models, map Steam apps to IGDB games, or update the Steam-only mart.

## Stage IGDB Reference Data

After one or more successful raw IGDB reference runs, normalize the latest successful raw run for each available title slug:

```powershell
game-market-analytics stage-igdb-reference
```

To stage a single title:

```powershell
game-market-analytics stage-igdb-reference --title "Counter-Strike 2"
```

To stage a specific raw run directory or payload file:

```powershell
game-market-analytics stage-igdb-reference --raw-path data\raw\igdb\reference\title_slug=counter-strike-2\extract_date=YYYY-MM-DD\run_timestamp=YYYYMMDDTHHMMSSZ
```

Staged outputs land under:

```text
data/stage/igdb/reference/<ENTITY_NAME>/title_slug=<TITLE_SLUG>/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

Each entity-specific directory contains:

```text
<entity_name>.parquet
metadata.json
```

Current staged entities are:

- `games`
- `involved_companies`
- `companies`
- `genres`
- `platforms`
- `release_dates`

This command writes local Parquet only. It does not create dbt models, resolve Steam app IDs to IGDB game IDs, or enrich the final Steam mart.

## Stage the Steam App Catalog

After a successful raw ingestion run, normalize the latest successful raw extract:

```powershell
game-market-analytics stage-steam-app-catalog
```

Or:

```powershell
make stage-steam-app-catalog
```

By default, the command finds the latest successful raw app catalog extract. To stage a specific raw file:

```powershell
game-market-analytics stage-steam-app-catalog --raw-file data\raw\steam\app_catalog\extract_date=YYYY-MM-DD\run_timestamp=YYYYMMDDTHHMMSSZ\app_catalog.json
```

Staged output lands under:

```text
data/stage/steam/app_catalog/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

The directory contains:

```text
app_catalog.parquet
metadata.json
```

This command writes local Parquet only. It does not write to DuckDB tables or create dbt models.

## Stage Steam Reviews

After one or more successful raw review ingestion runs, normalize the latest successful run for each available app ID:

```powershell
game-market-analytics stage-steam-reviews
```

Or:

```powershell
make stage-steam-reviews
```

To stage one app ID:

```powershell
game-market-analytics stage-steam-reviews --app-id 570
```

To stage a specific raw run directory or page file:

```powershell
game-market-analytics stage-steam-reviews --raw-path data\raw\steam\reviews\app_id=570\extract_date=YYYY-MM-DD\run_timestamp=YYYYMMDDTHHMMSSZ
```

Staged output lands under:

```text
data/stage/steam/reviews/app_id=<APP_ID>/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

The directory contains:

```text
reviews.parquet
metadata.json
```

This command writes local Parquet only. It does not join reviews to the app catalog or calculate final reputation marts yet.

## dbt Profile Template

The repository includes a dbt profile example at:

```text
dbt/profiles.example.yml
```

For local use, copy it to `dbt/profiles.yml`:

```powershell
Copy-Item dbt\profiles.example.yml dbt\profiles.yml
```

The example points dbt to:

```text
.local/game_market_analytics.duckdb
```

Run dbt from the repository root:

```powershell
dbt debug --project-dir dbt --profiles-dir dbt
```

The Makefile target `dbt-debug` follows this repository-local profile convention.

You can also initialize the local profile with:

```powershell
make dbt-init-profile
```

## Run dbt Models

After raw ingestion and stage normalization have produced at least one staged Parquet file, run:

```powershell
make dbt-run
make dbt-test
```

Or run both models and tests together:

```powershell
make dbt-build
```

The first dbt models are:

- `stg_steam__app_catalog`: reads staged Steam app catalog Parquet.
- `int_steam__app_catalog_latest`: selects the latest available record per Steam app ID.
- `stg_steam__reviews`: reads staged Steam reviews Parquet.
- `int_steam__review_summary_latest`: summarizes the latest review snapshot per Steam app ID.
- `mart_steam__catalog_reputation_current`: joins the current Steam catalog to latest review reputation metrics.

These models read from staged Parquet under `data/stage/steam/` relative to the repository root and write dbt relations into the local DuckDB database configured by `dbt/profiles.yml`.

To build only the current catalog + reputation mart and its dependencies:

```powershell
dbt build --project-dir dbt --profiles-dir dbt --select +mart_steam__catalog_reputation_current
```

The validated Steam-only MVP examples use review app IDs `570` and `730`.

## Run dbt Models for IGDB Reference Data

After `game-market-analytics stage-igdb-reference` has produced staged IGDB Parquet files, run:

```powershell
dbt build --project-dir dbt --profiles-dir dbt --select stg_igdb__games stg_igdb__involved_companies stg_igdb__companies stg_igdb__genres stg_igdb__platforms stg_igdb__release_dates int_igdb__games_latest_by_title
```

The IGDB dbt layer includes:

- `stg_igdb__games`: source-shaped game reference rows.
- `stg_igdb__involved_companies`: source-shaped game-company relationship rows.
- `stg_igdb__companies`: source-shaped company rows.
- `stg_igdb__genres`: source-shaped genre rows.
- `stg_igdb__platforms`: source-shaped platform rows.
- `stg_igdb__release_dates`: source-shaped release date rows.
- `int_igdb__games_latest_by_title`: latest available IGDB game reference row per curated title slug.

These models read staged Parquet under `data/stage/igdb/reference/` and write dbt views into the local DuckDB database. They do not create Steam-to-IGDB mappings or enrich the Steam-only mart.

## Run dbt Steam-to-IGDB Crosswalk

After Steam catalog dbt models and IGDB reference dbt models are available, build the deterministic crosswalk:

```powershell
dbt build --project-dir dbt --profiles-dir dbt --select +int_crosswalk__steam_to_igdb_reference
```

The crosswalk layer includes:

- `int_steam__app_catalog_latest_titles`: latest Steam catalog records with deterministic title keys.
- `int_crosswalk__steam_to_igdb_reference`: one row per matched Steam app ID using conservative title-level matching.

This command does not enrich `mart_steam__catalog_reputation_current`. It only creates an auditable intermediate mapping foundation for future enrichment work.

## Run dbt Enriched Steam Mart

After the Steam mart, IGDB dbt models, and crosswalk are available, build the first IGDB-enriched Steam mart:

```powershell
dbt build --project-dir dbt --profiles-dir dbt --select +mart_steam__catalog_reputation_enriched_current
```

The enriched layer includes:

- `int_igdb__game_companies_current`: publisher and developer rollups per IGDB game.
- `int_igdb__game_genres_current`: genre rollups per IGDB game.
- `int_igdb__game_platforms_current`: platform rollups per IGDB game.
- `int_igdb__game_release_current`: release metadata per IGDB game.
- `mart_steam__catalog_reputation_enriched_current`: one row per Steam app, preserving unmatched Steam rows and adding IGDB metadata when a controlled crosswalk match exists.

If the local DuckDB file is open in another application such as DataGrip, close that connection before running dbt against the default local profile.

## Current Scope

The local baseline supports setup, validation, path visibility, raw Steam app catalog landing, Steam app catalog stage normalization, dbt models over staged Steam data, controlled raw Steam reviews ingestion, Steam reviews stage normalization, a Steam-only catalog + reputation mart, controlled raw IGDB reference ingestion, IGDB reference stage normalization, IGDB dbt staging models, a deterministic Steam-to-IGDB crosswalk foundation, and a first IGDB-enriched Steam mart. It does not include manual override mapping, broad IGDB coverage, or IsThereAnyDeal data yet.
