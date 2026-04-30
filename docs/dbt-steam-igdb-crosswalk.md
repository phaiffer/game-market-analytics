# dbt Steam-to-IGDB Crosswalk

This Phase 2 step adds a controlled, auditable crosswalk foundation between the latest Steam catalog records and the latest curated IGDB game reference rows.

It does not enrich the final Steam mart yet. The goal is to make title-level matches visible and testable before company, genre, platform, or publisher enrichment is introduced.

## Purpose

The crosswalk prepares the project for:

- manual override mappings when deterministic title matching is not enough
- enrichment of Steam catalog and reputation data with IGDB metadata
- future publisher, developer, genre, and platform analytics

## Models

`int_steam__app_catalog_latest_titles`

- Grain: one row per Steam `source_app_id` with a nonblank deterministic title key.
- Source: `int_steam__app_catalog_latest`.
- Purpose: adds reusable Steam title keys for matching.

`int_crosswalk__steam_to_igdb_reference`

- Grain: one row per matched Steam `source_app_id`.
- Sources: `int_steam__app_catalog_latest_titles` and `int_igdb__games_latest_by_title`.
- Purpose: records deterministic title-level matches between Steam and curated IGDB reference rows.

## Title Normalization

The shared dbt macros in `dbt/macros/title_normalization.sql` use simple deterministic rules:

- lowercase
- trim
- replace punctuation and separators with spaces
- collapse repeated whitespace
- produce a slug-style key by replacing spaces with hyphens

The normalization is intentionally conservative. It does not use fuzzy matching, probabilistic scoring, phonetic matching, or external matching libraries.

## Matching Strategy

The crosswalk currently supports:

- `normalized_exact`: Steam normalized title equals IGDB normalized title.
- `slug_exact`: Steam slug-style title equals IGDB title slug.

`normalized_exact` receives a `match_confidence` of `1.0`. `slug_exact` receives `0.95`. These are deterministic rule labels, not statistical probabilities.

The model includes audit fields such as:

- Steam app ID and app name
- Steam normalized title and slug-style title
- IGDB game ID and game name
- IGDB title slug
- Steam and IGDB extract/run timestamps
- raw file paths
- match method
- exact-title flag
- manual-override flag
- match confidence

## Run

Build the crosswalk and its dependencies:

```powershell
dbt build --project-dir dbt --profiles-dir dbt --select +int_crosswalk__steam_to_igdb_reference
```

The build runs tests for source app ID uniqueness, required IDs, match method accepted values, and match confidence presence.

## Deferred Work

This step does not implement:

- manual override seed files or mapping workflows
- fuzzy or probabilistic matching
- full-catalog entity resolution beyond deterministic title matches
- enrichment of `mart_steam__catalog_reputation_current`
- company, genre, platform, publisher, or developer marts
- dashboards, notebooks, orchestration, Docker, CI/CD, or cloud setup

Those steps remain intentionally deferred until the crosswalk can be reviewed and extended deliberately.
