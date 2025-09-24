{{ 
  config(
    materialized='table',
    schema='core'
  )
}}

select 
  view_name,
  view_definition,
  depends_on_tables,
  refresh_frequency,
  is_materialized,
  current_timestamp() as __rdp_timestamp
from {{ ref('view_creation') }}