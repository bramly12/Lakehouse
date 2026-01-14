{{ config(materialized='table') }}

with source as (
    select brut
    from {{ source('postgres_bronze', 'bronze') }}
),

parsed as (
    select
        (regexp_matches(brut, '^(\S+)', ''))[1]                        as ip,
        (regexp_matches(brut, '\[(.*?)\]', ''))[1]                     as log_datetime,
        (regexp_matches(brut, '"(\S+)', ''))[1]                        as method,
        (regexp_matches(brut, '"\S+ (\S+)', ''))[1]                    as path,
        (regexp_matches(brut, '"\S+ \S+ \S+" (\d{3})', ''))[1]         as status,
        (regexp_matches(brut, '" \d{3} (\d+)', ''))[1]                 as bytes,
        (regexp_matches(brut, '"(\S+://[^"]+)"', ''))[1]               as referer,
        (regexp_matches(brut, '"Mozilla.*"', ''))[1]                   as user_agent
    from source
)

select * from parsed
