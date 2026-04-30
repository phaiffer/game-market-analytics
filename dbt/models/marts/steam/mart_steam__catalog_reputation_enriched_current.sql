with steam_mart as (

    select *
    from {{ ref('mart_steam__catalog_reputation_current') }}

),

crosswalk as (

    select *
    from {{ ref('int_crosswalk__steam_to_igdb_reference') }}

),

companies as (

    select *
    from {{ ref('int_igdb__game_companies_current') }}

),

genres as (

    select *
    from {{ ref('int_igdb__game_genres_current') }}

),

platforms as (

    select *
    from {{ ref('int_igdb__game_platforms_current') }}

),

release_current as (

    select *
    from {{ ref('int_igdb__game_release_current') }}

)

select
    steam_mart.source_app_id,
    steam_mart.source_system,
    steam_mart.app_name,
    steam_mart.item_type,
    steam_mart.extract_date,
    steam_mart.run_timestamp,
    steam_mart.review_extract_date,
    steam_mart.review_run_timestamp,
    steam_mart.has_reviews,
    steam_mart.total_reviews,
    steam_mart.positive_reviews,
    steam_mart.negative_reviews,
    steam_mart.positive_review_ratio,
    steam_mart.reviews_with_text,
    steam_mart.avg_votes_up,
    steam_mart.avg_votes_funny,
    steam_mart.avg_weighted_vote_score,
    steam_mart.latest_review_created_at,
    steam_mart.latest_review_updated_at,
    steam_mart.review_volume_bucket,
    crosswalk.igdb_game_id is not null as has_igdb_match,
    crosswalk.igdb_game_id,
    crosswalk.igdb_game_name,
    crosswalk.igdb_title_slug,
    crosswalk.match_method,
    crosswalk.match_confidence,
    crosswalk.is_exact_title_match,
    crosswalk.is_manual_override,
    companies.publisher_names,
    companies.developer_names,
    companies.publisher_count,
    companies.developer_count,
    genres.genre_names,
    genres.genre_count,
    platforms.platform_names,
    platforms.platform_count,
    release_current.first_release_date,
    release_current.earliest_release_date,
    release_current.release_date_count
from steam_mart
left join crosswalk
    on steam_mart.source_app_id = crosswalk.source_app_id
left join companies
    on crosswalk.igdb_game_id = companies.igdb_game_id
left join genres
    on crosswalk.igdb_game_id = genres.igdb_game_id
left join platforms
    on crosswalk.igdb_game_id = platforms.igdb_game_id
left join release_current
    on crosswalk.igdb_game_id = release_current.igdb_game_id
