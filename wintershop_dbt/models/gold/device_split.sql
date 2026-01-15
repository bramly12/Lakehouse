{{ config(materialized='table') }}

select
    case
        when user_agent ~* 'Mobile|Android|iPhone|SmartTV|Tizen' then 'mobile'
        else 'desktop'
    end as device_type,
    count(*) as nb_visits
from {{ ref('logs_parsed') }}
group by device_type
