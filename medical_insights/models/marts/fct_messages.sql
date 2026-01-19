{{ config(materialized='table') }}

with base as (
    select
        m.message_id,
        m.channel_username,
        m.channel_slug,
        m.text,
        m.posted_at,
        m.views,
        m.media_type,
        m.message_length,
        m.has_image
    from {{ ref('stg_telegram_messages') }} m
)

select
    b.message_id,
    d.channel_id,
    dt.date_day,
    b.channel_slug,
    b.channel_username,
    b.text,
    b.views,
    b.media_type,
    b.message_length,
    b.has_image
from base b
left join {{ ref('dim_channels') }} d
    on b.channel_slug = d.channel_slug
left join {{ ref('dim_dates') }} dt
    on date_trunc('day', b.posted_at)::date = dt.date_day
