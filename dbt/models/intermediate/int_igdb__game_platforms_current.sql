with platforms as (

    select
        selected_igdb_game_id as igdb_game_id,
        platform_id,
        platform_name,
        run_timestamp,
        extract_date,
        raw_file_path
    from {{ ref('stg_igdb__platforms') }}
    where selected_igdb_game_id is not null
      and platform_id is not null
      and ingestion_status = 'success'
      and staging_status = 'success'

),

ranked as (

    select
        *,
        row_number() over (
            partition by igdb_game_id, platform_id
            order by run_timestamp desc, extract_date desc, raw_file_path desc
        ) as platform_rank
    from platforms

),

current_platforms as (

    select *
    from ranked
    where platform_rank = 1

)

select
    igdb_game_id,
    string_agg(platform_name, ', ' order by platform_name)
        filter (where platform_name is not null) as platform_names,
    count(distinct platform_id) as platform_count
from current_platforms
group by 1
