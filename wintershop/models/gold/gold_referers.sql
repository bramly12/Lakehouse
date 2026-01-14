{{ config(materialized='table') }}

with logs as (
    select
        referer
    from {{ ref('logs_parsed') }}
    where referer is not null
),

clean as (
    select
        referer,
        -- domaine (ex: skishop.local, www.google.com, search.yahoo.com)
        (regexp_match(referer, '^https?://([^/]+)'))[1] as referer_domain
    from logs
)

select
    referer_domain,
    referer,
    count(*) as nb_hits
from clean
group by 1,2
order by nb_hits desc