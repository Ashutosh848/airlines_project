{{ 
  config(
    materialized='table',
    schema='core'
  )
}}

select 
  template_name,
  file_pattern,
  expected_columns,
  data_quality_rules,
  target_table,
  priority,
  current_timestamp() as __rdp_timestamp
from {{ ref('template_detection') }}