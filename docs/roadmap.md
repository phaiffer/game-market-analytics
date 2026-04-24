# Roadmap

The roadmap is designed for a local-first portfolio project. Each phase should produce visible technical value without adding unnecessary infrastructure.

## Phase 1: MVP

Focus: establish a working local analytics pipeline for catalog, release, platform, and review-oriented analysis.

- Implement source configuration loading.
- Add Steam ingestion for a small, controlled set of catalog and review metadata.
- Add IGDB enrichment only where it clearly improves the core model.
- Land raw source extracts under `data/raw/`.
- Load staged data into local DuckDB.
- Build dbt staging models for source-shaped tables.
- Build initial intermediate models for normalized games, companies, platforms, genres, and themes.
- Build first marts for catalog, releases, and review snapshots.
- Add focused unit tests and data contract checks.

## Phase 2: Enrichment and Modeling

Focus: improve analytical consistency and source integration quality.

- Add documented entity matching rules.
- Introduce conformed game and company dimensions.
- Improve bridge tables for company roles, genres, themes, and platforms.
- Add daily snapshot patterns for review and catalog attributes.
- Expand source assessment based on real implementation findings.
- Add data quality checks for completeness, uniqueness, and accepted values.
- Prepare pricing model design for a later ITAD integration.

## Phase 3: Serving and Presentation

Focus: make the analysis easy to consume while staying local-first.

- Add curated notebooks for exploration and validation.
- Add lightweight exports from DuckDB marts for portfolio presentation.
- Create a small documentation-backed analysis narrative.
- Consider a simple dashboard only after marts are stable.
- Add ITAD pricing enrichment if matching quality is sufficient.

Cloud deployment, orchestration platforms, and CI/CD are intentionally out of scope unless the project requirements change.
