select
    scheme_code,
    nav_date
from {{ ref('stg_mf_nav') }}
group by scheme_code, nav_date
having count(*) > 1