# IGDB Reference Ingestion

This Phase 2 step adds a minimal, local-first IGDB raw ingestion foundation. It is designed to collect inspectable raw payloads for curated game titles before any staging, dbt modeling, or Steam-to-IGDB mapping is introduced.

## Purpose

The command lands raw IGDB reference data that can support future enrichment with:

- game metadata
- involved companies
- companies
- genres
- platforms
- release dates

The scope is intentionally title-driven. A human provides a small curated list of titles, the ingestion writes raw payloads, and later phases can decide how to normalize and map those payloads safely.

## Credentials

IGDB uses Twitch client-credentials authentication. Set these values in your active shell or local `.env` file:

```powershell
$env:IGDB_CLIENT_ID = "your-client-id"
$env:IGDB_CLIENT_SECRET = "your-client-secret"
```

Secrets are not hardcoded and are not included in normal error messages.

## CLI Usage

Repeated `--title` arguments:

```powershell
game-market-analytics ingest-igdb-reference --title "Dota 2" --title "Counter-Strike 2"
```

Input file with one title per line:

```powershell
game-market-analytics ingest-igdb-reference --input-file .local\igdb_titles.txt
```

Blank lines are ignored. Repeated titles are deduplicated after normalization while preserving first-seen order.

## Raw Landing Convention

Each title-specific run lands under:

```text
data/raw/igdb/reference/title_slug=<TITLE_SLUG>/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

Example:

```text
data/raw/igdb/reference/title_slug=dota-2/extract_date=2026-04-29/run_timestamp=20260429T153000Z/
```

The directory always includes:

```text
games_search.json
metadata.json
```

When a clean candidate is selected, the run can also include:

```text
game_details.json
involved_companies.json
companies.json
genres.json
platforms.json
release_dates.json
```

## Candidate Selection

The ingestion performs an IGDB game search for each input title and preserves the raw search response. It selects a candidate only when there is one exact normalized title match. If the search returns exactly one result, the command selects it cautiously and records a warning in `metadata.json`.

If no clean candidate is available, the command still writes `games_search.json` and `metadata.json`, but it does not fetch related entities.

## Metadata

Each title-specific run writes `metadata.json` with operational fields including:

- source name
- extraction type
- input title
- normalized title slug
- run timestamp
- status
- files written
- candidate game count
- selected game ID
- warnings
- error message, when applicable

The CLI continues processing later titles if one title fails, and prints a concise per-title summary.

## Deferred Work

This step does not implement:

- IGDB stage normalization
- dbt sources or models for IGDB
- automatic Steam-to-IGDB entity resolution
- enrichment of `mart_steam__catalog_reputation_current`
- dashboards, notebooks, orchestration, Docker, CI/CD, cloud setup, or fake data

Those are intentionally left for later phases after the raw payload shape has been inspected.
