{{ config(materialized='table') }}

with base as (
    select
        -- on extrait "/page?x=y" depuis la ligne brute
        split_part(split_part(brut, '"', 2), ' ', 2) as raw_path,
        http_method
    from {{ ref('stg_access') }}
),
clean as (
    select
        split_part(raw_path, '?', 1) as page_clean
    from base
    where http_method = 'GET'
)

select
    page_clean as page,
    count(*) as nb_hits
from clean
where page_clean is not null
  and page_clean <> ''
  and page_clean !~* '\.(css|js|png|jpg|jpeg|gif|svg|ico|woff|woff2|ttf|map)$'
group by 1
order by nb_hits desc
