{{ config(materialized='table') }}

select
    dt_ref,
    vl_selic,
    mm7,
    pct_change,

    case 
        when pct_change > 0 then 'alta'
        when pct_change < 0 then 'queda'
        else 'estavel'
    end as tendencia,

    case 
        when abs(pct_change) > 0.02 then 'alta volatilidade'
        when abs(pct_change) > 0.01 then 'media volatilidade'
        else 'baixa volatilidade'
    end as volatilidade

from {{ ref('int_selic') }}