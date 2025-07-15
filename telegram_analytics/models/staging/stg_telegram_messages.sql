{{ config(materialized = 'view') }} with source_data as (
    select *
    from {{ source('raw', 'telegram_messages') }}
),
cleaned_data as (
    select id as message_id,
        channel as channel_name,
        date::timestamp as message_date,
        text as message_text,
        coalesce(views, 0) as view_count,
        coalesce(forwards, 0) as forward_count,
        coalesce(replies, 0) as reply_count,
        coalesce(has_media, false) as has_media,
        scraped_at::timestamp as scraped_at,
        -- Extract date components for dimension table joins
        date::date as message_date_only,
        extract(
            year
            from date
        ) as message_year,
        extract(
            month
            from date
        ) as message_month,
        extract(
            day
            from date
        ) as message_day,
        extract(
            hour
            from date
        ) as message_hour,
        extract(
            dow
            from date
        ) as day_of_week,
        -- Text analysis metrics
        case
            when text is null
            or length(trim(text)) = 0 then 0
            else length(text)
        end as message_length,
        case
            when text is null
            or length(trim(text)) = 0 then 0
            else array_length(string_to_array(trim(text), ' '), 1)
        end as word_count,
        -- Engagement metrics
        case
            when views > 0 then round((forwards::decimal / views) * 100, 2)
            else 0
        end as forward_rate,
        case
            when views > 0 then round((replies::decimal / views) * 100, 2)
            else 0
        end as reply_rate,
        -- Message classification
        case
            when text ~* '(http|https|www\.)' then 'contains_link'
            when text ~* '@[a-zA-Z0-9_]+' then 'mentions_user'
            when text ~* '#[a-zA-Z0-9_]+' then 'contains_hashtag'
            when has_media = true then 'media_message'
            when length(text) > 500 then 'long_text'
            else 'regular_text'
        end as message_type,
        -- Data quality flags
        case
            when text is null
            or length(trim(text)) = 0 then true
            else false
        end as is_empty_message,
        current_timestamp as dbt_updated_at
    from source_data
    where id is not null
        and channel is not null
        and date is not null
)
select *
from cleaned_data