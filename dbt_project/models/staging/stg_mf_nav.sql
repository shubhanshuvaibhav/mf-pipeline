with raw_source_records as (
    select
        ingested_at,
        scheme_code,
        (raw_payload->'meta'->>'scheme_name') as fund_name,
        (raw_payload->'meta'->>'fund_house') as fund_house,
        x.date as nav_date_raw,
        x.nav as nav_raw
    from {{ source('bronze_source', 'raw_dump') }}
    cross join lateral jsonb_to_recordset(raw_payload->'data') as x(
        date varchar,
        nav varchar
    )
), ranked_source_records as (
    select
        *,
        row_number() over(
            partition by scheme_code, nav_date_raw
            order by ingested_at desc
        ) as record_rank
    from raw_source_records
)

select
    md5(scheme_code) as fund_key,
    scheme_code,
    trim(fund_name) as fund_name,
    trim(fund_house) as asset_management_company_name,
    to_date(nav_date_raw, 'DD-MM-YYYY') as nav_date,
    cast(nav_raw as numeric) as net_asset_value_inr
from ranked_source_records
where record_rank = 1
