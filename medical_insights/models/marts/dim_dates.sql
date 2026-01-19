{{ config(materialized='table') }}

with source as (
    select distinct
        date_trunc('day', posted_at)::date as date_day
    from {{ ref('stg_telegram_messages') }}
)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(week from date_day) as week,
    extract(dow from date_day) as weekday
from source
