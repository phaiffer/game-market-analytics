# Local Setup

This project is designed to run locally on Windows with a small Python, DuckDB, and dbt toolchain.

## Python Version

Use Python 3.11 or newer.

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

The API credential values are placeholders for future source integrations. They are not used by the current local utilities.

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

The current official Steam Store app list endpoint requires a Steam Web API key. Add it to your active shell or local `.env` file:

```powershell
$env:STEAM_API_KEY = "your-key-here"
```

The command fetches the app catalog endpoint, writes the raw JSON response, and writes run metadata next to the payload.

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

When running dbt from the `dbt/` directory, use:

```powershell
dbt debug --profiles-dir .
```

The Makefile target `dbt-debug` follows this repository-local profile convention.

## Current Scope

The local baseline supports setup, validation, path visibility, future DuckDB/dbt development, raw Steam app catalog landing, and Steam app catalog stage normalization. It does not ingest Steam reviews, IGDB, or IsThereAnyDeal data yet.
