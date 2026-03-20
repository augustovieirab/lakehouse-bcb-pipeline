{{ config(materialized='table') }}

select
    dt_ref,
    vl_selic,
    dt_load_bronze,
    ts_load_bronze,
    ts_load_silver,
    source,
    serie_codigo
from read_parquet('data_lake/silver/bcb/selic/**/*.parquet')