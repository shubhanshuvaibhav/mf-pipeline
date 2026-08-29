select *
from {{ ref('stg_mf_nav') }}
where net_asset_value_inr <= 0