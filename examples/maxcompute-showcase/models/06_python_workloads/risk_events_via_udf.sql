{{
  config(
    materialized='table',
    partition_by={'field': 'ds', 'data_type': 'string'},
    lifecycle=1,
    tags=['complex_python', 'catalog_udf']
  )
}}

select
  event_id,
  customer_id,
  {{ function('enrich_risk_event') }}(
    event_json,
    phone,
    event_time,
    default_region
  ) as risk_profile,
  ds
from {{ ref('risk_events') }}
