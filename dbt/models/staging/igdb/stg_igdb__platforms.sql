with source as (

    select * from {{ source('igdb_stage', 'platforms') }}

),

renamed as (

    select
        'igdb' as source_system,
        cast(platform_id as bigint) as platform_id,
        nullif(trim(cast(platform_name as varchar)), '') as platform_name,
        nullif(trim(cast(slug as varchar)), '') as slug,
        cast(category as bigint) as category,
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
