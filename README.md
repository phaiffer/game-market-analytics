# Game Market Analytics

A standalone portfolio repository for local-first game market analytics and gaming product analytics.

This project is intended to become a technically serious analytics engineering and data engineering case study around real-world game catalog data. The long-term goal is to analyze how games, publishers, developers, platforms, genres, themes, releases, reviews, and eventually prices shape market positioning.

The repository is currently in its foundation phase. It defines the documentation, project layout, configuration placeholders, Python package boundaries, and dbt scaffold needed for future implementation. It does not yet ingest external APIs, build production models, or publish dashboards.

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

No real API integration is implemented yet.

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

Implemented in the current foundation:

- Documentation foundation under `docs/`.
- Source, entity, and metric configuration placeholders under `config/`.
- Local data directory layout under `data/`.
- Python package scaffold under `src/game_market_analytics/`.
- dbt project scaffold under `dbt/` aimed at DuckDB.
- Test directory placeholders for unit tests and data contracts.
- Minimal project metadata in `pyproject.toml`.
- Safe local workflow targets and development utilities in `Makefile`.
- Example environment variable placeholders in `.env.example`.

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

- Source-specific ingestion clients for Steam and IGDB.
- Raw-to-stage normalization into DuckDB.
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
make test
make lint
make dbt-debug
```

The `dbt-debug` target expects a local `dbt/profiles.yml`, which can be created from `dbt/profiles.example.yml`.
