with source as (

    select * from {{ source('steam_stage', 'reviews') }}

),

renamed as (

    select
        cast(source_system as varchar) as source_system,
        cast(source_app_id as bigint) as source_app_id,
        nullif(trim(cast(review_id as varchar)), '') as review_id,
        cast(review_text as varchar) as review_text,
        nullif(trim(cast(language as varchar)), '') as language,
        nullif(trim(cast(review_score_desc as varchar)), '') as review_score_desc,
        cast(voted_up as boolean) as voted_up,
        cast(votes_up as bigint) as votes_up,
        cast(votes_funny as bigint) as votes_funny,
        cast(weighted_vote_score as double) as weighted_vote_score,
        cast(steam_purchase as boolean) as steam_purchase,
        cast(received_for_free as boolean) as received_for_free,
        cast(written_during_early_access as boolean) as written_during_early_access,
        nullif(trim(cast(author_steamid as varchar)), '') as author_steamid,
        cast(author_num_games_owned as bigint) as author_num_games_owned,
        cast(author_num_reviews as bigint) as author_num_reviews,
        cast(review_created_at as timestamp) as review_created_at,
        cast(review_updated_at as timestamp) as review_updated_at,
        cast(extract_date as date) as extract_date,
        cast(run_timestamp as varchar) as run_timestamp,
        cast(raw_file_path as varchar) as raw_file_path,
        cast(ingestion_status as varchar) as ingestion_status,
        nullif(trim(cast(review_text as varchar)), '') is not null as has_review_text
    from source

)

select * from renamed
