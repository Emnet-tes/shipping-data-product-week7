{{ config(materialized='table') }}

with date_range as (
    select 
        min(message_date_only) as min_date,
        max(message_date_only) as max_date
    from {{ ref('stg_telegram_messages') }}
),
date_spine as (
    select generate_series(
        (select min_date from date_range),
        (select max_date from date_range),
        interval '1 day'
    )::date as date_day
),
date_dim as (
    select 
        md5(date_day::text) as date_key,
        date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(quarter from date_day) as quarter,
        extract(dow from date_day) as day_of_week,
        extract(doy from date_day) as day_of_year,
        extract(week from date_day) as week_of_year,

        -- Formatted dates
        to_char(date_day, 'YYYY-MM-DD') as date_string,
        to_char(date_day, 'Month') as month_name,
        to_char(date_day, 'Day') as day_name,
        to_char(date_day, 'YYYY-MM') as year_month,
        to_char(date_day, 'YYYY-"Q"Q') as year_quarter,

        -- Business logic
        case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend,
        case when extract(dow from date_day) between 1 and 5 then true else false end as is_weekday,
        
        case extract(dow from date_day)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name_short,

        case extract(month from date_day)
            when 1 then 'January'
            when 2 then 'February'
            when 3 then 'March'
            when 4 then 'April'
            when 5 then 'May'
            when 6 then 'June'
            when 7 then 'July'
            when 8 then 'August'
            when 9 then 'September'
            when 10 then 'October'
            when 11 then 'November'
            when 12 then 'December'
        end as month_name_full,

        current_timestamp as dbt_updated_at
    from date_spine
)

select * from date_dim
