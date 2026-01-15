{{ config(materialized='table') }}

with base as (
    select
        log_datetime,
        path
    from {{ ref('logs_parsed') }}
    where path is not null
),

categorized as (
    select
        date_trunc('day', log_datetime) as day,
        case
            when path like '/skis/%' then 'skis'
            when path like '/snowboards/%' or path = '/snowboards/' then 'snowboards'
            when path like '/boots/%' or path = '/boots/' then 'boots'
            when path like '/helmets/%' or path = '/helmets/' then 'helmets'
            when path like '/goggles/%' or path = '/goggles/' then 'goggles'
            when path like '/gloves/%' then 'gloves'
            when path like '/jackets/%' then 'jackets'
            when path like '/pants/%' then 'pants'
            when path like '/bindings/%' or path = '/bindings/' then 'bindings'
            when path like '/bags/%' or path = '/bags/' then 'bags'
            when path like '/backpacks/%' then 'backpacks'
            when path like '/base-layers/%' or path = '/base-layers/' then 'base-layers'
            when path like '/mid-layers/%' or path = '/mid-layers/' then 'mid-layers'
            when path like '/socks/%' then 'socks'
            when path like '/brands/%' then 'brands'
            when path like '/blog/%' or path = '/blog/' then 'blog'
            when path like '/search%' then 'search'
            else 'other'
        end as category
    from base
)

select
    day,
    category,
    count(*) as nb_hits
from categorized
group by 1,2
order by day desc, nb_hits desc