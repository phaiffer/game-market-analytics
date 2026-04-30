with involved_companies as (

    select *
    from {{ ref('stg_igdb__involved_companies') }}
    where igdb_game_id is not null
      and company_id is not null
      and ingestion_status = 'success'
      and staging_status = 'success'

),

companies as (

    select *
    from {{ ref('stg_igdb__companies') }}
    where company_id is not null
      and ingestion_status = 'success'
      and staging_status = 'success'

),

joined as (

    select
        involved_companies.igdb_game_id,
        involved_companies.company_id,
        companies.company_name,
        coalesce(involved_companies.publisher_flag, false) as publisher_flag,
        coalesce(involved_companies.developer_flag, false) as developer_flag,
        involved_companies.run_timestamp,
        involved_companies.extract_date,
        involved_companies.raw_file_path
    from involved_companies
    left join companies
        on involved_companies.company_id = companies.company_id
        and involved_companies.selected_igdb_game_id = companies.selected_igdb_game_id

),

ranked as (

    select
        *,
        row_number() over (
            partition by igdb_game_id, company_id
            order by run_timestamp desc, extract_date desc, raw_file_path desc
        ) as company_rank
    from joined

),

current_companies as (

    select *
    from ranked
    where company_rank = 1

)

select
    igdb_game_id,
    string_agg(company_name, ', ' order by company_name)
        filter (where publisher_flag and company_name is not null) as publisher_names,
    string_agg(company_name, ', ' order by company_name)
        filter (where developer_flag and company_name is not null) as developer_names,
    count(distinct case when publisher_flag then company_id end) as publisher_count,
    count(distinct case when developer_flag then company_id end) as developer_count
from current_companies
group by 1
