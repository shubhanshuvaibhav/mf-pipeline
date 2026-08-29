with ranked_funds as (
    select
        fund_key,
        scheme_code,
        fund_name,
        asset_management_company_name,
        nav_date,
        row_number() over(
            partition by scheme_code
            order by nav_date desc
        ) as record_rank
    from {{ ref('stg_mf_nav') }}
)

select
    fund_key,
    scheme_code,
    fund_name,
    asset_management_company_name
from ranked_funds
where record_rank = 1