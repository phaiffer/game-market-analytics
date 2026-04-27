# Game Market Analytics

A standalone portfolio repository for local-first game market analytics and gaming product analytics.

This project is intended to become a technically serious analytics engineering and data engineering case study around real-world game catalog data. The long-term goal is to analyze how games, publishers, developers, platforms, genres, themes, releases, reviews, and eventually prices shape market positioning.

The repository is currently in an early implementation phase. It defines the documentation, project layout, configuration placeholders, Python package boundaries, and dbt scaffold needed for future implementation. It includes Steam app catalog raw landing, stage normalization, initial dbt models over staged Parquet, controlled Steam reviews raw ingestion for selected app IDs, and Steam reviews stage normalization. It does not yet publish dashboards.

## Domain Focus

The project centers on the commercial and product analytics questions behind game markets:

- Which games exist in the catalog, and how are they classified?
- Which publishers and developers are associated with each game?
- How do genre, theme, and platform coverage vary over time?
- How are releases distributed by date, company, platform, and market segment?
- How do public review aggregates and reputation signals evolve?
- How might pricing and discount behavior be added in later phases?

## MVP Scope

The planned MVP is intentionally narrow and local-first:

- Build a curated game catalog from source-specific records.
- Normalize core entities such as games, companies, platforms, genres, and themes.
- Model release trends and review/reputation aggregates.
- Store analytical data locally with DuckDB.
- Transform source-shaped data into analytics-friendly tables with dbt.
- Document source assumptions, entity grains, and candidate metrics.

The MVP will prioritize clarity, reproducibility, and business usefulness over infrastructure complexity.

## Planned Sources

The project is designed to support these future sources:

- **Steam**: game catalog, store metadata, platforms, release dates, and review aggregate signals.
- **IGDB**: richer metadata for games, companies, genres, themes, platforms, and release context.
- **IsThereAnyDeal**: future pricing, discount, and deal history enrichment.

Current implementation note: the Steam app catalog endpoint is implemented through raw landing, stage normalization, and dbt modeling. Steam reviews are implemented through controlled raw ingestion and stage normalization for explicitly provided app IDs. IGDB and IsThereAnyDeal are still planned work.

## Architecture Direction

The intended architecture is a simple local analytics stack:

1. Source data lands under `data/raw/` by source system.
2. Python modules under `src/game_market_analytics/` handle future ingestion, normalization, matching, and quality checks.
3. Local DuckDB files hold staged and modeled data.
4. dbt models under `dbt/models/` define staging, intermediate, and mart layers.
5. Documentation and YAML configuration describe sources, business entities, and candidate metrics.

This shape keeps the project interview-friendly: the technical choices are visible, but the foundation does not add cloud services, containers, orchestration platforms, or deployment machinery before they are needed.

## Why This Matters

Game market data is analytically rich because it combines catalog metadata, product taxonomy, company relationships, release timing, player reputation, and eventually price behavior. From a data engineering perspective, it creates realistic challenges:

- Integrating multiple source systems with different identifiers and grains.
- Handling many-to-many relationships between games, companies, genres, themes, and platforms.
- Separating raw source records from normalized and conformed analytical entities.
- Designing snapshots for changing review and catalog attributes.
- Building metrics that remain meaningful when source coverage is incomplete.

From an analytics engineering perspective, the domain supports clear dimensional modeling decisions and business-facing KPIs.

## Current Repository Foundation

Implemented in the current foundation and local baseline:

- Documentation foundation under `docs/`.
- Source, entity, and metric configuration placeholders under `config/`.
- Local data directory layout under `data/`.
- Python package scaffold under `src/game_market_analytics/`.
- dbt project scaffold under `dbt/` aimed at DuckDB.
- Test directory placeholders and focused unit tests for local utilities and raw ingestion helpers.
- Minimal project metadata in `pyproject.toml`.
- Safe local workflow targets and development utilities in `Makefile`.
- Example environment variable placeholders in `.env.example`.
- Real Steam app catalog raw ingestion under `src/game_market_analytics/ingestion/steam/`.
- Steam app catalog stage normalization to Parquet under `data/stage/`.
- Initial dbt models for Steam app catalog staging and latest catalog records.
- Controlled Steam reviews raw ingestion for parameterized app IDs.
- Steam reviews stage normalization to Parquet under `data/stage/`.

## Implemented Ingestion

The first implemented source flow is the Steam application catalog.

Run it locally after setup:

```powershell
game-market-analytics ingest-steam-app-catalog
```

Or through the Makefile:

```powershell
make ingest-steam-app-catalog
```

The command fetches the official Steam app list payload and lands the raw JSON response under:

```text
data/raw/steam/app_catalog/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/app_catalog.json
```

Each successful run also writes a sidecar metadata file:

```text
data/raw/steam/app_catalog/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/metadata.json
```

This flow preserves the source payload as-is for future normalization and staging work.

The current official Steam Web API app list endpoint requires a Steam Web API key. Set `STEAM_API_KEY` and `STEAM_API_KEY_AUTH_LOCATION=query` in your shell or local `.env` file before running the command. By default the key is sent with Steam's `key` query parameter; set `STEAM_API_KEY_AUTH_LOCATION=header` only if you need to test the `x-webapi-key` header format.

After raw ingestion, normalize the latest successful raw extract into staged Parquet:

```powershell
game-market-analytics stage-steam-app-catalog
```

Or through the Makefile:

```powershell
make stage-steam-app-catalog
```

The staged dataset lands under:

```text
data/stage/steam/app_catalog/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/app_catalog.parquet
```

Each staged run also writes:

```text
data/stage/steam/app_catalog/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/metadata.json
```

The first dbt models read this staged Parquet dataset:

- `stg_steam__app_catalog`: source-shaped staging model.
- `int_steam__app_catalog_latest`: latest available record per Steam app ID.

Initialize the repository-local dbt profile, then run dbt:

```powershell
make dbt-init-profile
make dbt-build
```

The dbt workflow is repository-root based. The profile writes to `.local/game_market_analytics.duckdb`, and the Steam stage source reads `data/stage/steam/app_catalog/**/*.parquet`.

Steam reviews can also be landed for a controlled app subset:

```powershell
game-market-analytics ingest-steam-reviews --app-id 570 --app-id 730 --max-pages 1
```

Review payloads land under:

```text
data/raw/steam/reviews/app_id=<APP_ID>/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/
```

Normalize the latest successful raw review runs into staged Parquet:

```powershell
game-market-analytics stage-steam-reviews
```

The staged review dataset lands under:

```text
data/stage/steam/reviews/app_id=<APP_ID>/extract_date=YYYY-MM-DD/run_timestamp=YYYYMMDDTHHMMSSZ/reviews.parquet
```

The first review dbt models read this staged Parquet dataset:

- `stg_steam__reviews`: source-shaped review staging model.
- `int_steam__review_summary_latest`: latest review summary per Steam app ID.

The first Steam-only mart joins catalog and reputation:

- `mart_steam__catalog_reputation_current`: one current row per Steam app with latest catalog fields and latest review summary metrics.

## Local Setup

The local development baseline now includes a small CLI for setup and validation. See `docs/setup.md` for the full walkthrough.

Typical Windows setup:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
game-market-analytics init-local
game-market-analytics validate-project
```

The repository-local DuckDB convention is:

```text
.local/game_market_analytics.duckdb
```

This file is ignored by Git and is intended for future local dbt and DuckDB development.

## Planned Future Implementation

Future phases may add:

- Additional Steam endpoints such as review aggregates.
- Broader Steam review/reputation marts and historical trend models.
- Source-specific ingestion clients for IGDB.
- Broader business marts over the Steam catalog.
- Entity matching and conformed game/company dimensions.
- dbt staging, intermediate, and mart models.
- Data quality checks and data contract tests.
- Pricing enrichment from IsThereAnyDeal.
- Lightweight local presentation assets or notebooks.

The project will remain local-first unless a later requirement clearly justifies additional infrastructure.

## Repository Layout

```text
config/                     Source, entity, and metric definitions
data/                       Local raw, stage, and mart storage paths
dbt/                        dbt project scaffold for DuckDB transformations
docs/                       Domain, architecture, model, KPI, and roadmap docs
notebooks/                  Local exploratory analysis notebooks
src/game_market_analytics/  Python package scaffold
tests/                      Unit and data contract test placeholders
```

## Local Workflow

The Makefile contains small local workflow commands:

```powershell
make setup
make init-local
make validate
make show-paths
make ingest-steam-app-catalog
make ingest-steam-reviews
make stage-steam-app-catalog
make stage-steam-reviews
make test
make lint
make dbt-init-profile
make dbt-debug
make dbt-run
make dbt-test
make dbt-build
```

The `dbt-debug` target expects a local `dbt/profiles.yml`, which can be created from `dbt/profiles.example.yml`. Direct dbt commands should be run from the repository root with `--project-dir dbt --profiles-dir dbt`.
