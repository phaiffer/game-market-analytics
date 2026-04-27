# dbt Steam Reviews Models

The Steam reviews dbt layer makes staged review Parquet queryable in DuckDB and creates the first review summary dataset for reputation analysis.

## Source

The dbt source `steam_stage.reviews` reads staged Parquet files from:

```text
data/stage/steam/reviews/**/*.parquet
```

This keeps dbt downstream of the Python raw-to-stage flow. dbt does not read raw JSON review pages.

## Models

### stg_steam__reviews

Source-shaped staging view over staged Steam review Parquet. It casts fields into expected types, preserves review text and source fields, and adds `has_review_text`.

The grain is one staged Steam review record per `review_id`, raw page, app ID, and run timestamp.

### int_steam__review_summary_latest

Latest available review summary per `source_app_id`. It aggregates the latest staged review snapshot for each app into review counts, positive and negative counts, positive review ratio, text coverage, vote averages, and latest review timestamps.

The grain is one row per Steam app ID.

## Run

After review ingestion and review staging:

```powershell
game-market-analytics ingest-steam-reviews --app-id 570 --max-pages 1
game-market-analytics stage-steam-reviews --app-id 570
make dbt-build
```

The review summary is intentionally separate from the app catalog for now. A later mart can join `int_steam__review_summary_latest` to `int_steam__app_catalog_latest`.
