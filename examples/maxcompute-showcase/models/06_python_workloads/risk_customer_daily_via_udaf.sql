{{
  config(
    materialized='table',
    partition_by={'field': 'ds', 'data_type': 'string'},
    lifecycle=1,
    tags=['complex_python', 'catalog_udf']
  )
}}

select
  customer_id,
  {{ function('merge_risk_signals') }}(risk_profile) as risk_rollup,
  ds
from {{ ref('risk_events_via_udf') }}
group by customer_id, ds
