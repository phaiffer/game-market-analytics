with games as (

    select *
    from {{ ref('int_igdb__games_latest_by_title') }}

),

release_dates as (

    select
        igdb_game_id,
        release_date_id,
        release_date_timestamp,
        run_timestamp,
        extract_date,
        raw_file_path
    from {{ ref('stg_igdb__release_dates') }}
    where igdb_game_id is not null
      and release_date_id is not null
      and ingestion_status = 'success'
      and staging_status = 'success'

),

ranked as (

    select
        *,
        row_number() over (
            partition by igdb_game_id, release_date_id
            order by run_timestamp desc, extract_date desc, raw_file_path desc
        ) as release_date_rank
    from release_dates

),

current_release_dates as (

    select *
    from ranked
    where release_date_rank = 1

),

aggregated_release_dates as (

    select
        igdb_game_id,
        min(release_date_timestamp) as earliest_release_date,
        count(distinct release_date_id) as release_date_count
    from current_release_dates
    group by 1

)

select
    games.igdb_game_id,
    games.first_release_date,
    aggregated_release_dates.earliest_release_date,
    coalesce(aggregated_release_dates.release_date_count, 0) as release_date_count
from games
left join aggregated_release_dates
    on games.igdb_game_id = aggregated_release_dates.igdb_game_id
