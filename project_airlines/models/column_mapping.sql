{{ 
  config(
    materialized='table',
    schema='core'
  )
}}

select 
  source_table,
  source_column,
  target_table,
  target_column,
  transformation_rule,
  data_type,
  is_primary_key,
  current_timestamp() as __rdp_timestamp
from {{ ref('column_mapping') }}