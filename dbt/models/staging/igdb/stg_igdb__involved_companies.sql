with source as (

    select * from {{ source('igdb_stage', 'involved_companies') }}

),

renamed as (

    select
        'igdb' as source_system,
        cast(involved_company_id as bigint) as involved_company_id,
        cast(igdb_game_id as bigint) as igdb_game_id,
        cast(company_id as bigint) as company_id,
        cast(developer_flag as boolean) as developer_flag,
        cast(publisher_flag as boolean) as publisher_flag,
        cast(supporting_flag as boolean) as supporting_flag,
        cast(porting_flag as boolean) as porting_flag,
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
