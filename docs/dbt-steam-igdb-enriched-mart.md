# dbt Steam IGDB Enriched Mart

This step adds the first IGDB enrichment layer and a final enriched Steam mart. It uses the validated Steam-to-IGDB crosswalk to add companies, genres, platforms, and release metadata to the current Steam catalog and reputation mart.

The implementation is intentionally conservative: it only enriches Steam rows that already have a deterministic crosswalk match. It does not add fuzzy matching or broaden entity resolution.

## Inputs

Primary base mart:

- `mart_steam__catalog_reputation_current`

Crosswalk:

- `int_crosswalk__steam_to_igdb_reference`

IGDB staging models:

- `stg_igdb__involved_companies`
- `stg_igdb__companies`
- `stg_igdb__genres`
- `stg_igdb__platforms`
- `stg_igdb__release_dates`
- `int_igdb__games_latest_by_title`

## Helper Models

`int_igdb__game_companies_current`

- Grain: one row per `igdb_game_id`.
- Purpose: aggregates publisher and developer company names from IGDB involved company relationships.
- Key fields: `publisher_names`, `developer_names`, `publisher_count`, `developer_count`.

`int_igdb__game_genres_current`

- Grain: one row per `igdb_game_id`.
- Purpose: aggregates IGDB genre names.
- Key fields: `genre_names`, `genre_count`.

`int_igdb__game_platforms_current`

- Grain: one row per `igdb_game_id`.
- Purpose: aggregates IGDB platform names.
- Key fields: `platform_names`, `platform_count`.

`int_igdb__game_release_current`

- Grain: one row per `igdb_game_id`.
- Purpose: exposes conservative release metadata from the latest IGDB game reference row and staged release date rows.
- Key fields: `first_release_date`, `earliest_release_date`, `release_date_count`.

## Final Mart

`mart_steam__catalog_reputation_enriched_current`

Grain:

- one row per Steam `source_app_id`

Purpose:

- preserve the current Steam catalog and reputation metrics
- retain every Steam mart row, even when no IGDB match exists
- add IGDB enrichment fields when a controlled crosswalk match exists

Selected enrichment fields:

- `has_igdb_match`
- `igdb_game_id`
- `igdb_game_name`
- `igdb_title_slug`
- `match_method`
- `match_confidence`
- `publisher_names`
- `developer_names`
- `genre_names`
- `platform_names`
- `first_release_date`
- `earliest_release_date`

When no IGDB match exists, `has_igdb_match` is `false` and enrichment fields remain null.

## Run

Build the enriched mart and its dependencies:

```powershell
dbt build --project-dir dbt --profiles-dir dbt --select +mart_steam__catalog_reputation_enriched_current
```

If `.local/game_market_analytics.duckdb` is open in DataGrip or another process, close that connection before running dbt with the default local profile.

## Current Limitations

- IGDB enrichment is limited to rows covered by the deterministic crosswalk.
- The current validated local examples are Dota 2 and Counter-Strike 2.
- Company, genre, platform, and release values are aggregated as readable lists and counts, not dimensional marts.
- Release dates remain IGDB timestamp values; downstream presentation formatting is deferred.

## Deferred Work

This step does not implement:

- manual override mapping
- fuzzy or probabilistic matching
- broader IGDB coverage beyond curated titles
- dedicated company, genre, platform, or publisher marts
- dashboards, notebooks, orchestration, Docker, CI/CD, or cloud setup

Those steps are left for later phases after the enriched mart is validated and reviewed.
