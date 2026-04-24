# dbt Steam App Catalog Models

The first dbt layer makes the staged Steam app catalog queryable in DuckDB and introduces a small modeling path for future analytics.

## Local Profile

Use Python 3.11 or 3.12 for the dbt workflow.

The project uses a repository-local dbt profile:

```text
dbt/profiles.yml
```

Create it from the example:

```powershell
make dbt-init-profile
```

The profile points to:

```text
.local/game_market_analytics.duckdb
```

## Source

The dbt source `steam_stage.app_catalog` reads staged Parquet files from:

```text
../data/stage/steam/app_catalog/**/*.parquet
```

This keeps dbt downstream of the Python raw-to-stage flow. dbt does not read raw JSON.

## Models

### stg_steam__app_catalog

Source-shaped staging view over staged Steam app catalog Parquet. It casts fields into expected types, standardizes blank app names to null, and adds `is_valid_app_record`.

### int_steam__app_catalog_latest

Latest available Steam catalog record per `source_app_id`. This model is the first business-oriented dataset in the repository and is suitable for basic catalog exploration.

## Run

After ingestion and staging:

```powershell
make dbt-build
```

This runs the models and lightweight tests.
