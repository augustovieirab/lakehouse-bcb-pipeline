{{ config(materialized='table') }}

with windowed as (
    select
        dt_ref,
        vl_selic,
        avg(vl_selic) over (
            order by dt_ref
            rows between 6 preceding and current row
        ) as mm7,
        lag(vl_selic) over (order by dt_ref) as vl_prev
    from {{ ref('stg_selic') }}
)

select
    dt_ref,
    vl_selic,
    mm7,
    vl_prev,
    case
        when vl_prev is null or vl_prev = 0 then null
        else (vl_selic - vl_prev) / vl_prev
    end as pct_change
from windowed
where dt_ref is not null
order by dt_ref