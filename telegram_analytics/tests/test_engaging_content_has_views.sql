-- Test to ensure that messages with engagement have non-zero view counts
-- This test should return 0 rows to pass
select message_id,
    channel_key,
    view_count,
    forward_count,
    reply_count,
    is_engaging_content
from {{ ref('fct_messages') }}
where is_engaging_content = true
    and view_count = 0