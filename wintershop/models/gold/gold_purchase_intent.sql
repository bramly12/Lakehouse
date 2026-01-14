{{ config(materialized='table') }}

with base as (
    select
        log_datetime,
        path
    from {{ ref('logs_parsed') }}
    where path is not null
),

intent as (
    select
        date_trunc('day', log_datetime) as day,
        case
            -- pages qui signalent une intention d’achat forte
            when path in ('/sale-items', '/deals', '/clearance', '/bestsellers', '/gift-guide') then 'High intent - promo/guide'
            when path like '/search%' then 'Intent - search'
            when path like '/payment-methods%' or path like '/invoice%' or path like '/order-status%' then 'Checkout / after-sales'
            -- pages produit (consultation catalogue)
            when path like '/skis/%' or path like '/snowboards/%' or path like '/boots/%'
              or path like '/helmets/%' or path like '/goggles/%' or path like '/gloves/%'
              or path like '/jackets/%' or path like '/pants/%' or path like '/bindings/%'
              or path like '/bags/%' or path like '/backpacks/%' then 'Product view'
            -- contenu (moins “achat”, plus “info”)
            when path like '/blog/%' then 'Content / blog'
            else 'Other navigation'
        end as intent_stage
    from base
)

select
    day,
    intent_stage,
    count(*) as nb_hits
from intent
group by 1,2
order by day desc, nb_hits desc