{{ config(materialized='table') }}

with source as (
    select brut
    from {{ source('bronze', 'bronze') }}
),

parsed as (
    select
        (regexp_match(brut, '^(\S+)'))[1] as ip,

        -- texte entre [ ] (ex: 14/Jan/2026:00:20:06 )
        (regexp_match(brut, '\[([^\]]+)\]'))[1] as log_datetime,

        -- "GET /path HTTP/1.1"
        (regexp_match(brut, '"(\S+)\s+(\S+)\s+(\S+)"'))[1] as method,
        (regexp_match(brut, '"(\S+)\s+(\S+)\s+(\S+)"'))[2] as path,

        -- status + bytes après la requête
        ((regexp_match(brut, '"\s+(\d{3})\s+(\d+)'))[1])::int as status,
        ((regexp_match(brut, '"\s+(\d{3})\s+(\d+)'))[2])::bigint as bytes,

        -- referer (peut être "-" ou une URL)
        nullif((regexp_match(brut, '"\s+\d{3}\s+\d+\s+"([^"]*)"'))[1], '-') as referer,

        -- user agent = dernier champ entre guillemets
        (regexp_match(brut, '"\s+\d{3}\s+\d+\s+"[^"]*"\s+"([^"]*)"'))[1] as user_agent

    from source
)

select * from parsed