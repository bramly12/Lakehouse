{{ config(materialized='table') }}

with logs as (
    select
        referer
    from {{ ref('logs_parsed') }}
    where referer is not null
),

google as (
    select
        referer,
        -- extrait le param q=... dans l’URL
        (regexp_match(referer, '[?&]q=([^&]+)'))[1] as q_raw
    from logs
    where referer ilike '%google.%'
      and referer ilike '%/search%'
),

clean as (
    select
        referer,
        -- nettoyage simple: + -> espace
        replace(q_raw, '+', ' ') as query
    from google
    where q_raw is not null
)

select
    query,
    count(*) as nb_hits
from clean
group by 1
order by nb_hits desc