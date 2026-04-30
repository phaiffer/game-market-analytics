with genres as (

    select
        selected_igdb_game_id as igdb_game_id,
        genre_id,
        genre_name,
        run_timestamp,
        extract_date,
        raw_file_path
    from {{ ref('stg_igdb__genres') }}
    where selected_igdb_game_id is not null
      and genre_id is not null
      and ingestion_status = 'success'
      and staging_status = 'success'

),

ranked as (

    select
        *,
        row_number() over (
            partition by igdb_game_id, genre_id
            order by run_timestamp desc, extract_date desc, raw_file_path desc
        ) as genre_rank
    from genres

),

current_genres as (

    select *
    from ranked
    where genre_rank = 1

)

select
    igdb_game_id,
    string_agg(genre_name, ', ' order by genre_name)
        filter (where genre_name is not null) as genre_names,
    count(distinct genre_id) as genre_count
from current_genres
group by 1
