with staged as (

    select *
    from {{ ref('stg_steam__reviews') }}
    where ingestion_status = 'success'
      and source_app_id is not null

),

snapshots as (

    select
        source_system,
        source_app_id,
        extract_date,
        run_timestamp,
        count(*) as total_reviews,
        sum(case when voted_up then 1 else 0 end) as positive_reviews,
        sum(case when voted_up = false then 1 else 0 end) as negative_reviews,
        sum(case when has_review_text then 1 else 0 end) as reviews_with_text,
        avg(votes_up) as avg_votes_up,
        avg(votes_funny) as avg_votes_funny,
        avg(weighted_vote_score) as avg_weighted_vote_score,
        max(review_created_at) as latest_review_created_at,
        max(review_updated_at) as latest_review_updated_at
    from staged
    group by 1, 2, 3, 4

),

ranked as (

    select
        *,
        row_number() over (
            partition by source_app_id
            order by run_timestamp desc, extract_date desc
        ) as review_snapshot_rank
    from snapshots

)

select
    source_system,
    source_app_id,
    extract_date,
    run_timestamp,
    total_reviews,
    positive_reviews,
    negative_reviews,
    positive_reviews::double / nullif(positive_reviews + negative_reviews, 0) as positive_review_ratio,
    reviews_with_text,
    avg_votes_up,
    avg_votes_funny,
    avg_weighted_vote_score,
    latest_review_created_at,
    latest_review_updated_at
from ranked
where review_snapshot_rank = 1
