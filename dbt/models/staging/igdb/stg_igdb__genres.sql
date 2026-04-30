with source as (

    select * from {{ source('igdb_stage', 'genres') }}

),

renamed as (

    select
        'igdb' as source_system,
        cast(genre_id as bigint) as genre_id,
        nullif(trim(cast(genre_name as varchar)), '') as genre_name,
        nullif(trim(cast(slug as varchar)), '') as slug,
        nullif(trim(cast(input_title as varchar)), '') as input_title,
        nullif(trim(cast(title_slug as varchar)), '') as title_slug,
        cast(selected_igdb_game_id as bigint) as selected_igdb_game_id,
        cast(extract_date as date) as extract_date,
        cast(run_timestamp as varchar) as run_timestamp,
        cast(raw_file_path as varchar) as raw_file_path,
        cast(ingestion_status as varchar) as ingestion_status,
        cast(staging_status as varchar) as staging_status
    from source

)

select * from renamed
