{{ config(materialized='table') }}

select
    path,
    count(*) as nb_errors
from {{ ref('logs_parsed') }}
where status::int >= 400
group by path
order by nb_errors desc
