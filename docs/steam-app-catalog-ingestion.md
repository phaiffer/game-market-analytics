# Steam App Catalog Ingestion

The first implemented source onboarding flow is the Steam application catalog raw landing process.

## What It Does

The command fetches the official Steam app list payload and stores the raw JSON response locally. It also writes a small metadata file for the run.

The current official Steam Store app list endpoint requires a Steam Web API key. Set `STEAM_API_KEY` in your shell or local `.env` file before running the command. The default authentication format sends the key as Steam's `key` query parameter.

Optional header mode is available for compatibility checks:

```powershell
$env:STEAM_API_KEY_AUTH_LOCATION = "header"
```

Valid values are `query` and `header`. Header mode sends the key as `x-webapi-key`.

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

The raw payload can be normalized with:

```powershell
game-market-analytics stage-steam-app-catalog
```

## HTTP 403 Troubleshooting

If Steam returns HTTP 403, the client reports whether the request used query parameter auth or header auth. Check for a missing or invalid `STEAM_API_KEY`, a revoked key, an authorization mismatch for the official app catalog endpoint, or a request format mismatch. Error messages do not include the secret key value.

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
