with source as (

    select * from {{ source('igdb_stage', 'companies') }}

),

renamed as (

    select
        'igdb' as source_system,
        cast(company_id as bigint) as company_id,
        nullif(trim(cast(company_name as varchar)), '') as company_name,
        nullif(trim(cast(slug as varchar)), '') as slug,
        cast(country as bigint) as country,
        cast(start_date as bigint) as start_date,
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
