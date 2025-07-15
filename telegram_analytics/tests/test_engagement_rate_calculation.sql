-- Test to ensure forward and reply rates are calculated correctly
-- This test should return 0 rows to pass
select message_id,
    view_count,
    forward_count,
    reply_count,
    forward_rate,
    reply_rate
from {{ ref('fct_messages') }}
where view_count > 0
    and (
        forward_rate != round((forward_count::decimal / view_count) * 100, 2)
        or reply_rate != round((reply_count::decimal / view_count) * 100, 2)
    )