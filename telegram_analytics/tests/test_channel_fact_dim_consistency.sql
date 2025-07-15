-- Test to ensure channel statistics are consistent with fact table
-- This test should return 0 rows to pass
with channel_fact_stats as (
    select channel_key,
        count(*) as fact_message_count,
        sum(view_count) as fact_total_views
    from {{ ref('fct_messages') }}
    group by channel_key
),
channel_dim_stats as (
    select channel_key,
        total_messages as dim_message_count,
        total_views as dim_total_views
    from {{ ref('dim_channels') }}
)
select f.channel_key,
    f.fact_message_count,
    d.dim_message_count,
    f.fact_total_views,
    d.dim_total_views
from channel_fact_stats f
    join channel_dim_stats d on f.channel_key = d.channel_key
where f.fact_message_count != d.dim_message_count
    or f.fact_total_views != d.dim_total_views