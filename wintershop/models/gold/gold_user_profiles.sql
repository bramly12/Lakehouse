{{ config(materialized='table') }}

with logs as (
    select
        user_agent
    from {{ ref('logs_parsed') }}
),

classified as (
    select
        case
            when user_agent ilike '%bot%' then 'Bot / Crawler'
            when user_agent ilike '%android%'
              or user_agent ilike '%iphone%'
              or user_agent ilike '%mobile%' then 'Mobile User'
            when user_agent ilike '%windows%'
              or user_agent ilike '%macintosh%'
              or user_agent ilike '%linux%' then 'Desktop User'
            else 'Other'
        end as user_profile
    from logs
)

select
    user_profile,
    count(*) as nb_hits
from classified
group by user_profile
order by nb_hits desc