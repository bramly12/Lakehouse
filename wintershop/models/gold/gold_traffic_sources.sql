{{ config(materialized='table') }}

with logs as (
    select
        referer
    from {{ ref('logs_parsed') }}
),

classified as (
    select
        case
            when referer is null then 'Direct / Unknown'
            when referer ilike '%google.%' then 'SEO - Google'
            when referer ilike '%bing.%'
              or referer ilike '%yahoo.%'
              or referer ilike '%duckduckgo.%' then 'SEO - Other'
            when referer ilike '%skishop.local%' then 'Internal Navigation'
            else 'External Websites'
        end as traffic_source
    from logs
)

select
    traffic_source,
    count(*) as nb_hits
from classified
group by traffic_source
order by nb_hits desc