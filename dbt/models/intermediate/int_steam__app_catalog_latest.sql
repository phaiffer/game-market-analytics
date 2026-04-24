with staged as (

    select *
    from {{ ref('stg_steam__app_catalog') }}
    where is_valid_app_record

),

ranked as (

    select
        *,
        row_number() over (
            partition by source_app_id
            order by run_timestamp desc, extract_date desc, raw_file_path desc
        ) as app_record_rank
    from staged

)

select
    source_system,
    source_app_id,
    app_name,
    item_type,
    last_modified,
    price_change_number,
    extract_date,
    run_timestamp,
    raw_file_path,
    ingestion_status
from ranked
where app_record_rank = 1
