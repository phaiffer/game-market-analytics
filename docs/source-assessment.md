# Source Assessment

This document describes the planned source systems at a high level. It avoids implementation-specific assumptions until source integrations are designed and tested.

## Steam

### Role in the Project

Steam is expected to be a primary MVP source for catalog and market-facing store metadata.

### Expected Data Domains

- Game and application catalog records
- Store-facing titles and descriptions
- Release dates
- Supported platforms
- Genres, categories, or tags where available
- Review aggregate signals where available

### Strengths

- Strong relevance to PC game market analysis
- Useful store-oriented metadata
- Practical source for release and reputation-oriented analytics
- Good fit for an MVP centered on catalog and review trends

### Limitations

- Source coverage reflects Steam, not the entire game market
- Metadata may be store-oriented rather than analytically normalized
- Some attributes may be incomplete, inconsistent, or change over time
- Cross-source identity matching will still be required later

### MVP Fit

Steam belongs in the MVP because it can support catalog, platform, release, and review-oriented analysis with a clear product analytics story.

## IGDB

### Role in the Project

IGDB is expected to enrich the game catalog with broader game metadata and stronger taxonomy coverage.

### Expected Data Domains

- Games and alternative names
- Companies and company roles
- Genres and themes
- Platforms
- Release information
- External identifiers where available

### Strengths

- Rich game metadata orientation
- Useful taxonomy for genres, themes, companies, and platforms
- Helpful for building a broader conceptual game model
- Strong candidate for entity enrichment and matching

### Limitations

- Coverage and completeness may differ from Steam
- Identifier mapping must be designed carefully
- Source-specific taxonomy may not map one-to-one to other systems
- API usage and authentication details should be validated during implementation

### MVP Fit

IGDB is a strong candidate for MVP or early enrichment because it supports the dimensional model beyond a store-only view.

## IsThereAnyDeal

### Role in the Project

IsThereAnyDeal is a candidate later-phase source for pricing, discounts, and deal history.

### Expected Data Domains

- Price observations
- Discount signals
- Store availability
- Deal or offer metadata
- Historical pricing context where available

### Strengths

- Adds commercial pricing behavior to the analytical model
- Enables price and discount trend analysis
- Complements catalog and reputation analytics with market economics

### Limitations

- Pricing adds additional grain and snapshot complexity
- Store and game matching must be mature before pricing is reliable
- It is not necessary for the first catalog and reputation MVP

### Phase Fit

IsThereAnyDeal belongs in a later phase after catalog identity, game matching, and core marts are stable.
