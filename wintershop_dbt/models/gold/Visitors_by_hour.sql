{{ config(materialized='table') }}

with hourly as (
    select
        date_trunc('hour', to_timestamp(log_datetime, 'DD/Mon/YYYY:HH24:MI:SS')) as hour,
        count(distinct ip) as visitors
    from {{ ref('logs_parsed') }}
    group by hour
)

select
    min(visitors) as min_visitors,
    max(visitors) as max_visitors,
    avg(visitors)::int as avg_visitors
from hourly
