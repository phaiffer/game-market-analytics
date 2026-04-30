with latest_catalog as (

    select *
    from {{ ref('int_steam__app_catalog_latest') }}

)

select
    source_system,
    source_app_id,
    app_name as steam_app_name,
    {{ normalize_game_title('app_name') }} as steam_title_normalized,
    {{ slugify_game_title('app_name') }} as steam_title_slug,
    item_type,
    extract_date,
    run_timestamp,
    raw_file_path,
    ingestion_status
from latest_catalog
where app_name is not null
  and {{ normalize_game_title('app_name') }} is not null
