{{ config(materialized='table') }}

with source as (
    select brut
    from {{ source('bronze', 'bronze') }}
),

extracted as (
    select
        brut,

        (regexp_match(brut, '^(\S+)'))[1] as ip,

        -- ex: 14/Jan/2026:00:20:06  (il y a parfois un espace avant ])
        trim((regexp_match(brut, '\[([^\]]+)\]'))[1]) as log_datetime_txt,

        -- user entre les 2 tirets : "-" ou un username
        nullif((regexp_match(brut, '^\S+\s+-\s+(\S+)'))[1], '-') as user_name,

        -- "GET /path HTTP/1.1"
        (regexp_match(brut, '"(\S+)\s+(\S+)\s+(\S+)"'))[1] as method,
        (regexp_match(brut, '"(\S+)\s+(\S+)\s+(\S+)"'))[2] as path,
        (regexp_match(brut, '"(\S+)\s+(\S+)\s+(\S+)"'))[3] as http_version,

        -- status + bytes
        (regexp_match(brut, '"\s+(\d{3})\s+(\d+)'))[1] as status_txt,
        (regexp_match(brut, '"\s+(\d{3})\s+(\d+)'))[2] as bytes_txt,

        -- referer + user agent
        nullif((regexp_match(brut, '"\s+\d{3}\s+\d+\s+"([^"]*)"'))[1], '-') as referer,
        (regexp_match(brut, '"\s+\d{3}\s+\d+\s+"[^"]*"\s+"([^"]*)"'))[1] as user_agent

    from source
),

typed as (
    select
        ip,

        -- conversion texte -> timestamp
        to_timestamp(log_datetime_txt, 'DD/Mon/YYYY:HH24:MI:SS') as log_datetime,

        user_name,
        method,
        path,
        http_version,

        status_txt::int as status,
        bytes_txt::bigint as bytes,

        referer,
        user_agent

    from extracted
    where log_datetime_txt is not null
)

select * from typed