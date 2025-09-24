{{ 
  config(
    materialized='table',
    schema='core',
    pre_hook="{{ ensure_schema_exists('bronze', 'core') }}"
  )
}}

select 
  source_system,
  source_table,
  target_schema,
  extraction_pattern,
  incremental_column,
  is_full_load,
  current_timestamp() as __rdp_timestamp
from {{ ref('table_extraction_file1') }}

union all

select 
  source_system,
  source_table,
  target_schema,
  extraction_pattern,
  incremental_column,
  is_full_load,     
  current_timestamp() as __rdp_timestamp
from {{ ref('table_extraction_file2') }}