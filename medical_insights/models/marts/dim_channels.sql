{{ config(materialized='table') }}
    
with source as (
    select distinct
        channel_username,
        channel_title,
        replace(channel_username, '@', '') as channel_slug
    from {{ ref('stg_telegram_messages') }}
    where channel_username is not null
)

select
    row_number() over (order by channel_username) as channel_id,
    channel_username,
    channel_title,
    channel_slug
from source
