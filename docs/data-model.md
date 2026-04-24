# Conceptual Data Model

The initial model will start with source-specific records and move gradually toward conformed analytical entities. This avoids premature matching logic while keeping the design ready for multi-source analytics.

## Modeling Principles

- Preserve source-specific IDs before creating conformed entities.
- Keep many-to-many relationships in bridge tables.
- Treat changing source attributes and review aggregates as snapshots.
- Separate source-shaped staging models from business-facing marts.
- Delay entity resolution until source coverage and matching rules are better understood.

## Candidate Dimensions

### dim_game

One row per conformed game when entity resolution exists. Before that, source-specific game dimensions may be created in staging or intermediate layers.

Candidate attributes:

- game_key
- source_system_key
- source_game_id
- title
- normalized_title
- first_release_date
- created_at
- updated_at

### dim_company

Companies associated with games as developers, publishers, or other source-defined roles.

Candidate attributes:

- company_key
- source_company_id
- company_name
- normalized_company_name
- country or region where available

### dim_platform

Platforms on which games are released or supported.

Candidate attributes:

- platform_key
- source_platform_id
- platform_name
- platform_family

### dim_genre

Genre taxonomy used for product segmentation.

Candidate attributes:

- genre_key
- source_genre_id
- genre_name
- source_system_key

### dim_theme

Theme taxonomy used to describe setting, narrative, or subject matter.

Candidate attributes:

- theme_key
- source_theme_id
- theme_name
- source_system_key

### dim_date

Shared calendar dimension for release dates, snapshot dates, and future pricing observations.

### dim_source_system

Reference dimension describing source systems such as Steam, IGDB, and IsThereAnyDeal.

## Candidate Bridge Tables

### bridge_game_company_role

Represents the many-to-many relationship between games and companies with a role such as developer or publisher.

### bridge_game_genre

Represents the many-to-many relationship between games and genres.

### bridge_game_theme

Represents the many-to-many relationship between games and themes.

### bridge_game_platform

Represents the many-to-many relationship between games and platforms.

## Candidate Facts

### fact_game_release

One row per game release event at the chosen grain. The grain may include game, platform, region, and source system depending on source availability.

### fact_game_review_snapshot_daily

Daily snapshot of review aggregate signals for a game or source-specific game record.

Candidate measures:

- review_count
- positive_review_count
- negative_review_count
- positive_review_ratio
- review_score_bucket

### fact_game_catalog_snapshot_daily

Daily snapshot of catalog availability and selected source attributes.

Candidate measures and flags:

- is_active_in_source
- has_release_date
- has_platform_metadata
- has_review_metadata

### fact_game_price_snapshot_daily

Future pricing fact for deal and price observations. This is intentionally out of MVP scope until source matching is mature.

Candidate measures:

- list_price
- current_price
- discount_percent
- observed_at

## Entity Resolution Direction

The first implementation should use source-specific IDs and source-specific staging models. Conformed game and company entities should come later, after matching rules are documented and tested. Matching may use title normalization, release dates, platforms, external IDs, and company context, but no matching logic is implemented in this foundation step.
