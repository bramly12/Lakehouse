{{ config(materialized='table') }}

with sessions as (
    select
        ip,
        min(to_timestamp(log_datetime, 'DD/Mon/YYYY:HH24:MI:SS')) as start_time,
        max(to_timestamp(log_datetime, 'DD/Mon/YYYY:HH24:MI:SS')) as end_time
    from {{ ref('logs_parsed') }}
    group by ip
)

select
    avg(end_time - start_time) as avg_session_duration
from sessions
