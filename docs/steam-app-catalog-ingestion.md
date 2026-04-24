# Steam App Catalog Ingestion

The first implemented source onboarding flow is the Steam application catalog raw landing process.

## What It Does

The command fetches the official Steam app list payload and stores the raw JSON response locally. It also writes a small metadata file for the run.

The current official Steam Store app list endpoint requires a Steam Web API key. Set `STEAM_API_KEY` in your shell or local `.env` file before running the command.

Run:

```powershell
game-market-analytics ingest-steam-app-catalog
```

Or:

```powershell
make ingest-steam-app-catalog
```

## Raw Landing Convention

Successful runs are written under:

```text
data/raw/steam/app_catalog/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

Files written:

```text
app_catalog.json
metadata.json
```

The partition-style path supports multiple runs, is easy to inspect locally, and prepares the project for future raw-to-stage processing.

## Metadata

The sidecar `metadata.json` includes:

- source name
- extraction type
- run timestamp
- endpoint
- status
- item count
- output file path
- metadata file path

## Current Boundaries

This flow intentionally does not:

- fetch Steam reviews
- call other Steam endpoints
- normalize the raw payload
- write to DuckDB
- create dbt models
- join to IGDB or IsThereAnyDeal

Those steps are reserved for later phases.
