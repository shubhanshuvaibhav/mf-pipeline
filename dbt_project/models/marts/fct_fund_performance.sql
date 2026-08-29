with staging_data as (
    select * from {{ ref('stg_mf_nav') }}
),

computed_metrics as (
    select
        fund_key,
        scheme_code,
        nav_date,
        net_asset_value_inr,
        lag(net_asset_value_inr) over(
            partition by fund_key 
            order by nav_date, scheme_code
        ) as previous_day_net_asset_value_inr,
        avg(net_asset_value_inr) over(
            partition by fund_key 
            order by nav_date, scheme_code
            rows between 29 preceding and current row
        ) as rolling_30_day_average_nav_inr
    from staging_data
)

select
    fund_key,
    scheme_code,
    nav_date,
    cast(net_asset_value_inr as numeric(18, 8)) as net_asset_value_inr,
    cast(rolling_30_day_average_nav_inr as numeric(18, 8)) as rolling_30_day_average_nav_inr,
    cast(case 
        when previous_day_net_asset_value_inr is not null
             and nullif(previous_day_net_asset_value_inr, 0) is not null
        then ((net_asset_value_inr - previous_day_net_asset_value_inr)
            / nullif(previous_day_net_asset_value_inr, 0)) * 100
        else 0 
    end as numeric(18, 8)) as daily_return_percentage
from computed_metrics
