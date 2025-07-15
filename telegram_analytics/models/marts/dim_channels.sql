{{ config(materialized='table') }}

with channel_stats as (
    select 
        channel_name,
        count(*) as total_messages,
        min(message_date) as first_message_date,
        max(message_date) as last_message_date,
        avg(view_count) as avg_views,
        avg(forward_count) as avg_forwards,
        avg(reply_count) as avg_replies,
        avg(message_length) as avg_message_length,
        sum(case when has_media then 1 else 0 end) as media_messages_count,
        sum(view_count) as total_views,
        sum(forward_count) as total_forwards,
        sum(reply_count) as total_replies,
        sum(case when is_empty_message then 1 else 0 end) as empty_messages_count
    from {{ ref('stg_telegram_messages') }}
    group by channel_name
),
channel_dim as (
    select 
        md5(channel_name) as channel_key,
        channel_name,
        total_messages,
        first_message_date,
        last_message_date,
        extract(day from (last_message_date - first_message_date)) + 1 as days_active,
        round(avg_views, 2) as avg_views_per_message,
        round(avg_forwards, 2) as avg_forwards_per_message,
        round(avg_replies, 2) as avg_replies_per_message,
        round(avg_message_length, 0) as avg_message_length,
        media_messages_count,
        empty_messages_count,
        round((media_messages_count::decimal / total_messages) * 100, 2) as media_message_percentage,
        round((empty_messages_count::decimal / total_messages) * 100, 2) as empty_message_percentage,
        total_views,
        total_forwards,
        total_replies,
        case
            when total_views > 0 then round((total_forwards::decimal / total_views) * 100, 2)
            else 0
        end as overall_forward_rate,
        case
            when total_views > 0 then round((total_replies::decimal / total_views) * 100, 2)
            else 0
        end as overall_reply_rate,
        case
            when total_messages >= 100 then 'high_volume'
            when total_messages >= 50 then 'medium_volume'
            else 'low_volume'
        end as channel_activity_level,
        case
            when avg_views >= 1000 then 'high_reach'
            when avg_views >= 500 then 'medium_reach'
            else 'low_reach'
        end as channel_reach_category,
        current_timestamp as dbt_updated_at
    from channel_stats
)

select * from channel_dim
