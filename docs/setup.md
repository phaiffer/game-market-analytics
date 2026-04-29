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

`STEAM_API_KEY` is used by the Steam app catalog ingestion flow. The IGDB and IsThereAnyDeal credential values remain placeholders for future source integrations.

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

## Current Scope

The local baseline supports setup, validation, path visibility, raw Steam app catalog landing, Steam app catalog stage normalization, dbt models over staged Steam data, controlled raw Steam reviews ingestion, Steam reviews stage normalization, and a Steam-only catalog + reputation mart. It does not ingest IGDB or IsThereAnyDeal data yet.
