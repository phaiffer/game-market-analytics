# Steam App Catalog Staging

The Steam app catalog staging flow normalizes a raw Steam app catalog extract into an analytics-friendly Parquet dataset.

## What It Does

The command reads `app_catalog.json` from the raw Steam landing area, flattens app records into stage rows, and writes a Parquet file plus metadata.

Run:

```powershell
game-market-analytics stage-steam-app-catalog
```

Or:

```powershell
make stage-steam-app-catalog
```

By default, it stages the latest successful raw extract based on the raw `metadata.json` files. A specific raw file can be provided:

```powershell
game-market-analytics stage-steam-app-catalog --raw-file path\to\app_catalog.json
```

## Stage Output Convention

Staged output is written under:

```text
data/stage/steam/app_catalog/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

Files written:

```text
app_catalog.parquet
metadata.json
```

## Staged Schema

The staged Parquet dataset includes:

- `source_system`
- `source_app_id`
- `app_name`
- `item_type`
- `last_modified`
- `price_change_number`
- `extract_date`
- `run_timestamp`
- `raw_file_path`
- `ingestion_status`

The schema is intentionally source-shaped. It prepares the project for future DuckDB loading and dbt modeling without creating conformed dimensions yet.

## Current Boundaries

This flow intentionally does not:

- fetch Steam reviews
- call IGDB or IsThereAnyDeal
- write to DuckDB tables
- create dbt models
- perform entity resolution
- build final dimensional marts
