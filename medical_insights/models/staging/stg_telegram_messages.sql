{{ config(
    materialized='view'
    ) }}

with raw as (
    select
        channel_title,
        channel_username,
        concat(replace(channel_username, '@', ''), '_', id::text) as message_id,  -- Concatenating for unique message ID
        text,
        date::timestamp as posted_at,
        views::integer,
        media_type,
        replace(channel_username, '@', '') as channel_slug
    from raw.telegram_messages

)

select
    channel_title,
    channel_username,
    message_id,                                     --  Renaming for clarity and downstream joins
    text,
    posted_at,                                      --  Cleaner naming for time dimensions
    views,
    media_type,
    length(text) as message_length,                 --  Calculating message length for analysis
    media_type = 'photo' as has_image,              --  Boolean flag for image presence
    channel_slug

from raw
