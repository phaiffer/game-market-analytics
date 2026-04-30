with steam_titles as (

    select *
    from {{ ref('int_steam__app_catalog_latest_titles') }}
    where steam_title_normalized is not null
      and ingestion_status = 'success'

),

igdb_titles as (

    select
        *,
        {{ normalize_game_title('game_name') }} as igdb_title_normalized,
        {{ slugify_game_title('game_name') }} as igdb_title_slug_from_name
    from {{ ref('int_igdb__games_latest_by_title') }}
    where igdb_game_id is not null
      and title_slug is not null
      and ingestion_status = 'success'
      and staging_status = 'success'

),

matched as (

    select
        steam_titles.source_app_id,
        steam_titles.steam_app_name,
        steam_titles.steam_title_normalized,
        steam_titles.steam_title_slug,
        steam_titles.extract_date as steam_extract_date,
        steam_titles.run_timestamp as steam_run_timestamp,
        steam_titles.raw_file_path as steam_raw_file_path,
        igdb_titles.igdb_game_id,
        igdb_titles.game_name as igdb_game_name,
        igdb_titles.igdb_title_normalized,
        igdb_titles.title_slug as igdb_title_slug,
        igdb_titles.extract_date as igdb_extract_date,
        igdb_titles.run_timestamp as igdb_run_timestamp,
        igdb_titles.raw_file_path as igdb_raw_file_path,
        case
            when steam_titles.steam_title_normalized = igdb_titles.igdb_title_normalized
                then 'normalized_exact'
            when steam_titles.steam_title_slug = igdb_titles.title_slug
                then 'slug_exact'
        end as match_method,
        steam_titles.steam_title_normalized = igdb_titles.igdb_title_normalized
            as is_exact_title_match,
        false as is_manual_override,
        case
            when steam_titles.steam_title_normalized = igdb_titles.igdb_title_normalized
                then 1.0
            when steam_titles.steam_title_slug = igdb_titles.title_slug
                then 0.95
        end as match_confidence
    from steam_titles
    inner join igdb_titles
        on steam_titles.steam_title_normalized = igdb_titles.igdb_title_normalized
        or steam_titles.steam_title_slug = igdb_titles.title_slug

),

ranked as (

    select
        *,
        row_number() over (
            partition by source_app_id
            order by
                match_confidence desc,
                igdb_run_timestamp desc,
                igdb_game_id
        ) as match_rank
    from matched

)

select
    source_app_id,
    steam_app_name,
    steam_title_normalized,
    steam_title_slug,
    steam_extract_date,
    steam_run_timestamp,
    steam_raw_file_path,
    igdb_game_id,
    igdb_game_name,
    igdb_title_normalized,
    igdb_title_slug,
    igdb_extract_date,
    igdb_run_timestamp,
    igdb_raw_file_path,
    match_method,
    is_exact_title_match,
    is_manual_override,
    match_confidence
from ranked
where match_rank = 1
