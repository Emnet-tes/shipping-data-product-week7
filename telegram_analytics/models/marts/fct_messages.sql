{{ config(materialized = 'table') }}

with message_facts as (
    select 
        stg.message_id,
        -- Foreign keys to dimension tables
        dim_ch.channel_key,
        dim_dt.date_key,
        
        -- Message attributes
        stg.message_date,
        stg.message_hour,
        stg.message_text,
        stg.message_type,
        stg.has_media,
        stg.is_empty_message,
        
        -- Core metrics
        stg.view_count,
        stg.forward_count,
        stg.reply_count,
        stg.message_length,
        stg.word_count,
        stg.forward_rate,
        stg.reply_rate,
        
        -- Calculated engagement metrics
        case
            when stg.view_count = 0 then 0
            else round(
                (stg.forward_count * 3 + stg.reply_count * 2 + stg.view_count)
                / nullif(stg.view_count, 0) * 100,
                2
            )
        end as engagement_score,
        
        -- Performance categorization
        case
            when stg.view_count >= 1000 then 'high_reach'
            when stg.view_count >= 500 then 'medium_reach'
            when stg.view_count >= 100 then 'low_reach'
            else 'minimal_reach'
        end as reach_category,
        
        case
            when stg.forward_rate >= 5 then 'high_viral'
            when stg.forward_rate >= 2 then 'medium_viral'
            when stg.forward_rate >= 0.5 then 'low_viral'
            else 'non_viral'
        end as virality_category,
        
        case
            when stg.reply_rate >= 3 then 'high_interaction'
            when stg.reply_rate >= 1 then 'medium_interaction'
            when stg.reply_rate >= 0.1 then 'low_interaction'
            else 'no_interaction'
        end as interaction_category,
        
        -- Time-based categorization
        case
            when stg.message_hour between 6 and 11 then 'morning'
            when stg.message_hour between 12 and 17 then 'afternoon'
            when stg.message_hour between 18 and 22 then 'evening'
            else 'night'
        end as time_of_day,
        
        -- Content categorization
        case
            when stg.message_length > 500 then 'long_form'
            when stg.message_length > 100 then 'medium_form'
            when stg.message_length > 0 then 'short_form'
            else 'empty'
        end as content_length_category,
        
        -- Quality flags
        case
            when stg.view_count = 0 and stg.forward_count = 0 and stg.reply_count = 0 then true
            else false
        end as is_no_engagement,
        
        case
            when stg.view_count > 0 and (stg.forward_count > 0 or stg.reply_count > 0) then true
            else false
        end as is_engaging_content,
        
        stg.scraped_at,
        current_timestamp as dbt_updated_at

    from {{ ref('stg_telegram_messages') }} stg
    left join {{ ref('dim_channels') }} dim_ch 
        on md5(stg.channel_name) = dim_ch.channel_key
    left join {{ ref('dim_dates') }} dim_dt 
        on md5(stg.message_date_only::text) = dim_dt.date_key
)

select * from message_facts
