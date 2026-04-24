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

The local baseline supports setup, validation, path visibility, and future DuckDB/dbt development. It does not ingest Steam, IGDB, or IsThereAnyDeal data yet.
