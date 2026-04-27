# Steam Reviews Staging

This flow normalizes raw Steam review page payloads into app-partitioned Parquet files for future dbt review models and reputation analytics.

It preserves source meaning and keeps review text as source-provided text. It does not perform sentiment analysis, language normalization, review aggregation, or joins to the Steam app catalog.

## Usage

Stage the latest successful raw review run for every available app ID:

```powershell
game-market-analytics stage-steam-reviews
```

Stage one app ID:

```powershell
game-market-analytics stage-steam-reviews --app-id 570
```

Stage a specific raw run directory or one raw page file:

```powershell
game-market-analytics stage-steam-reviews --raw-path data\raw\steam\reviews\app_id=570\extract_date=YYYY-MM-DD\run_timestamp=YYYYMMDDTHHMMSSZ
```

## Input Convention

Raw review runs are read from:

```text
data/raw/steam/reviews/app_id=<APP_ID>/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

The staging flow reads one or more files named:

```text
reviews_page_0001.json
reviews_page_0002.json
metadata.json
```

By default, only raw runs with `status: success` in metadata are selected. The latest successful run is selected independently for each app ID.

## Output Convention

Staged review datasets are written to:

```text
data/stage/steam/reviews/app_id=<APP_ID>/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

Files written:

```text
reviews.parquet
metadata.json
```

The metadata file records raw input paths, staged output path, app ID, row count, pages processed, run timestamp, status, and a schema summary.

## Staged Columns

The staged dataset includes:

- `source_system`
- `source_app_id`
- `review_id`
- `review_text`
- `language`
- `review_score_desc`
- `voted_up`
- `votes_up`
- `votes_funny`
- `weighted_vote_score`
- `steam_purchase`
- `received_for_free`
- `written_during_early_access`
- `author_steamid`
- `author_num_games_owned`
- `author_num_reviews`
- `review_created_at`
- `review_updated_at`
- `extract_date`
- `run_timestamp`
- `raw_file_path`
- `ingestion_status`

## Current Boundaries

This flow intentionally does not:

- create dbt sources or review models
- deduplicate reviews across multiple extracts
- calculate review or reputation metrics
- join reviews to `int_steam__app_catalog_latest`
- ingest IGDB or IsThereAnyDeal data
