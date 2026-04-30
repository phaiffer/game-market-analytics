with source as (

    select * from {{ source('igdb_stage', 'games') }}

),

renamed as (

    select
        'igdb' as source_system,
        cast(igdb_game_id as bigint) as igdb_game_id,
        nullif(trim(cast(game_name as varchar)), '') as game_name,
        nullif(trim(cast(slug as varchar)), '') as slug,
        cast(first_release_date as bigint) as first_release_date,
        cast(aggregated_rating as double) as aggregated_rating,
        cast(aggregated_rating_count as bigint) as aggregated_rating_count,
        cast(category as bigint) as category,
        nullif(trim(cast(input_title as varchar)), '') as input_title,
        nullif(trim(cast(title_slug as varchar)), '') as title_slug,
        cast(selected_igdb_game_id as bigint) as selected_igdb_game_id,
        cast(extract_date as date) as extract_date,
        cast(run_timestamp as varchar) as run_timestamp,
        cast(raw_file_path as varchar) as raw_file_path,
        cast(ingestion_status as varchar) as ingestion_status,
        cast(staging_status as varchar) as staging_status,
        igdb_game_id is not null
            and nullif(trim(cast(title_slug as varchar)), '') is not null
            as is_valid_game_reference
    from source

)

select * from renamed
