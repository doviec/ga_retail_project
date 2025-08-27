-- Fail if any session has a negative revenue (NULLs are allowed)
select *
from {{ ref('stg_sessions') }}
where revenue_usd is not null
  and revenue_usd < 0
