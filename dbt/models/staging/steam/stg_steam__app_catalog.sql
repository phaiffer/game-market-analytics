with source as (

    select * from {{ source('steam_stage', 'app_catalog') }}

),

renamed as (

    select
        cast(source_system as varchar) as source_system,
        cast(source_app_id as bigint) as source_app_id,
        nullif(trim(cast(app_name as varchar)), '') as app_name,
        nullif(trim(cast(item_type as varchar)), '') as item_type,
        cast(last_modified as bigint) as last_modified,
        cast(price_change_number as bigint) as price_change_number,
        cast(extract_date as date) as extract_date,
        cast(run_timestamp as varchar) as run_timestamp,
        cast(raw_file_path as varchar) as raw_file_path,
        cast(ingestion_status as varchar) as ingestion_status,
        source_app_id is not null
            and nullif(trim(cast(app_name as varchar)), '') is not null
            as is_valid_app_record
    from source

)

select * from renamed
