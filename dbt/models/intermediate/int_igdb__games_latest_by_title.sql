with staged as (

    select *
    from {{ ref('stg_igdb__games') }}
    where is_valid_game_reference
      and ingestion_status = 'success'
      and staging_status = 'success'

),

ranked as (

    select
        *,
        row_number() over (
            partition by title_slug
            order by run_timestamp desc, extract_date desc, raw_file_path desc
        ) as game_reference_rank
    from staged

)

select
    source_system,
    igdb_game_id,
    game_name,
    slug,
    first_release_date,
    aggregated_rating,
    aggregated_rating_count,
    category,
    input_title,
    title_slug,
    selected_igdb_game_id,
    extract_date,
    run_timestamp,
    raw_file_path,
    ingestion_status,
    staging_status
from ranked
where game_reference_rank = 1
