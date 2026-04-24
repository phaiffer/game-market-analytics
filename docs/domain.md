# Domain Overview

Game market analytics studies how games are positioned, released, categorized, reviewed, and maintained across storefronts and metadata systems. The domain is useful for product analytics because each game behaves like a marketable product with many descriptive and commercial attributes.

## Core Concepts

### Game Catalog

The catalog is the central inventory of games observed from source systems. A game may appear in multiple systems with different identifiers, titles, metadata completeness, and release details. Early project phases will preserve source-specific IDs before attempting conformed entity resolution.

### Publishers and Developers

Companies can appear in different roles, most commonly as publishers or developers. A single game may have multiple companies in multiple roles. This creates a many-to-many relationship that should be modeled explicitly rather than flattened into a single attribute.

### Genres, Themes, and Product Taxonomy

Genres describe gameplay categories, while themes describe subject matter, setting, or narrative framing. Both are useful for segmentation, but they are often source-defined and may require normalization before cross-source comparison.

### Platforms

Platforms describe where a game is available or released. Platform availability matters for catalog coverage, release strategy, and market reach. Platform definitions may vary by source and should be handled carefully.

### Release Trends

Release data enables analysis of market timing, seasonality, publisher activity, genre trends, and platform coverage. A game may have multiple release dates depending on platform, region, edition, or source coverage.

### Reviews and Reputation

Review aggregates provide reputation signals that can be analyzed by catalog segment, release cohort, publisher portfolio, platform, and genre. These signals should be treated as snapshots because they may change over time.

### Market Positioning

Market positioning combines product taxonomy, company relationships, release context, and reputation signals. The project can support questions such as which publishers specialize in certain genres, which segments receive stronger review signals, and how release patterns differ across platforms.

## Why This Domain Is Analytically Interesting

Game market data is realistic, messy, and business-relevant. It includes hierarchical classifications, many-to-many relationships, changing metrics, inconsistent source coverage, and source-specific identifiers. These are common challenges in professional analytics engineering work.

The domain is also accessible: stakeholders can understand questions about games, publishers, releases, reviews, and pricing without needing specialized financial or industrial context.
