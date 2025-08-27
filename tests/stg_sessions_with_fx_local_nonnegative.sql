-- Fail if any computed local revenue is negative (NULLs are fine)
select *
from {{ ref('stg_sessions_with_fx') }}
where revenue_local < 0
