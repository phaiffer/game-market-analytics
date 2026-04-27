with catalog as (

    select *
    from {{ ref('int_steam__app_catalog_latest') }}

),

reviews as (

    select *
    from {{ ref('int_steam__review_summary_latest') }}

)

select
    catalog.source_app_id,
    catalog.source_system,
    catalog.app_name,
    catalog.item_type,
    catalog.extract_date,
    catalog.run_timestamp,
    reviews.extract_date as review_extract_date,
    reviews.run_timestamp as review_run_timestamp,
    reviews.source_app_id is not null as has_reviews,
    coalesce(reviews.total_reviews, 0) as total_reviews,
    coalesce(reviews.positive_reviews, 0) as positive_reviews,
    coalesce(reviews.negative_reviews, 0) as negative_reviews,
    reviews.positive_review_ratio,
    coalesce(reviews.reviews_with_text, 0) as reviews_with_text,
    reviews.avg_votes_up,
    reviews.avg_votes_funny,
    reviews.avg_weighted_vote_score,
    reviews.latest_review_created_at,
    reviews.latest_review_updated_at,
    case
        when reviews.source_app_id is null then 'no_reviews'
        when reviews.total_reviews < 10 then '1_9'
        when reviews.total_reviews < 100 then '10_99'
        when reviews.total_reviews < 1000 then '100_999'
        else '1000_plus'
    end as review_volume_bucket
from catalog
left join reviews
    on catalog.source_app_id = reviews.source_app_id
