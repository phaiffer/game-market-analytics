# dbt Steam Catalog Reputation Mart

`mart_steam__catalog_reputation_current` is the first business-facing mart for the Steam-only MVP. It combines the latest Steam catalog record with the latest available review summary.

## Purpose

The mart gives one current-state row per Steam app with catalog identity and review reputation metrics in the same relation. It is intended for portfolio-friendly exploration and as a base for future catalog + reputation analysis.

## Grain

One row per `source_app_id`.

## Upstream Dependencies

The mart depends on:

- `int_steam__app_catalog_latest`
- `int_steam__review_summary_latest`

Catalog is the preserving side of the join. Apps remain in the mart even when no review summary has been staged yet.

## Null Handling

Review count fields are set to zero when no review summary exists. Review averages, review timestamps, and `positive_review_ratio` remain null when there is no matched review summary or no denominator. `has_reviews` makes review coverage explicit.

## Key Fields

- `source_app_id`
- `source_system`
- `app_name`
- `item_type`
- `extract_date`
- `run_timestamp`
- `has_reviews`
- `total_reviews`
- `positive_reviews`
- `negative_reviews`
- `positive_review_ratio`
- `reviews_with_text`
- `avg_votes_up`
- `avg_votes_funny`
- `avg_weighted_vote_score`
- `latest_review_created_at`
- `latest_review_updated_at`
- `review_volume_bucket`

## Run

After catalog staging, reviews staging, and dbt profile setup:

```powershell
make dbt-build
```

Or directly:

```powershell
dbt build --project-dir dbt --profiles-dir dbt --select mart_steam__catalog_reputation_current
```

## Current Boundaries

This mart intentionally does not include IGDB metadata, platforms, genres, publishers, pricing, dashboards, or historical review trend modeling. Those are later phases.
