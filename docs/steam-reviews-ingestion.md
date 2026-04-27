# Steam Reviews Ingestion

This flow lands raw Steam review payloads for a controlled set of Steam app IDs.

It is intentionally scoped. The command does not crawl the full Steam catalog. Staging to local Parquet is handled by the separate Steam reviews staging flow.

The flow uses Steam's documented `appreviews/<appid>` endpoint:

```text
https://partner.steamgames.com/doc/store/getreviews
```

## Why the Scope Is Controlled

Steam reviews can be large and cursor-paginated. A single popular game can have hundreds of thousands of reviews. This repository starts with explicit app IDs so extraction stays local-friendly, inspectable, and safe for portfolio development.

## Usage

Fetch reviews for repeated app IDs:

```powershell
game-market-analytics ingest-steam-reviews --app-id 570 --app-id 730
```

Fetch reviews from a text file:

```powershell
game-market-analytics ingest-steam-reviews --input-file .local/review_app_ids.txt
```

The input file should contain one Steam app ID per line. Blank lines and lines starting with `#` are ignored.

Optional controls:

```powershell
game-market-analytics ingest-steam-reviews --app-id 570 --max-pages 2 --language all --review-type all
```

Defaults:

- `--max-pages 1`
- `--language all`
- `--review-type all`
- `--filter recent`

## Raw Landing Convention

Each app ID gets its own run directory:

```text
data/raw/steam/reviews/app_id=570/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

Files written:

```text
reviews_page_0001.json
reviews_page_0002.json
metadata.json
```

Only fetched pages are written. With the default `--max-pages 1`, each successful app run writes one page plus metadata.

## Pagination

The flow uses Steam's cursor-based review pagination. It starts with cursor `*`, writes each raw response page, and stops when:

- the configured `--max-pages` limit is reached
- a page returns no review rows
- the response does not include a next cursor
- the next cursor repeats a previously seen cursor
- the request fails

The stop reason is recorded in `metadata.json`.

## Failure Behavior

If one app ID fails, the command writes failure metadata for that app and continues with the remaining app IDs. The CLI prints one summary line per app.

Invalid input, such as a non-integer app ID, stops before extraction begins.

## Current Boundaries

This flow intentionally does not:

- ingest reviews for the full Steam catalog
- create dbt review models
- join reviews to the app catalog
- calculate reputation metrics
- call IGDB or IsThereAnyDeal

Those steps are reserved for later phases.
