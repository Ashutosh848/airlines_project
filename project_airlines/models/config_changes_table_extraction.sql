
{{
  config(
    materialized='table',
    pre_hook="{{ process_table_extraction_changes() }}"
  )
}}

WITH config_changes AS (
  -- ADDED: Records in config_file but not in config_table
  SELECT 
    'ADDED' as change_type,
    cf.table_name,
    cf.schema_name,
    cf.template_name,
    cf.status
  FROM {{ ref('config_file') }} cf
  LEFT JOIN {{ ref('config_table') }} ct 
    ON cf.table_name = ct.table_name 
    AND cf.schema_name = ct.schema_name
    AND cf.template_name = ct.template_name
  WHERE ct.table_name IS NULL
  
  UNION ALL
  
  -- REMOVED: Records in config_table but not in config_file  
  SELECT 
    'REMOVED' as change_type,
    ct.table_name,
    ct.schema_name,
    ct.template_name,
    ct.status
  FROM {{ ref('config_table') }} ct
  LEFT JOIN {{ ref('config_file') }} cf
    ON ct.table_name = cf.table_name 
    AND ct.schema_name = cf.schema_name
    AND ct.template_name = cf.template_name
  WHERE cf.table_name IS NULL
)

SELECT 
  change_type,
  table_name,
  schema_name,
  template_name,
  status,
  current_timestamp() as detected_at
FROM config_changes