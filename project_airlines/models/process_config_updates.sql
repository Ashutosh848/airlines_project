{{ config(
    materialized='table',
    pre_hook="{{ execute_config_updates() }}"
) }}

WITH config_changes AS (
  SELECT 
    'ADDED' as change_type,
    cf.table_name,
    cf.schema_name,
    cf.template_name
  FROM {{ source('bronze_core', 'config_file') }} cf
  LEFT JOIN {{ source('bronze_core', 'config_table') }} ct 
    ON cf.table_name = ct.table_name 
    AND cf.schema_name = ct.schema_name
    AND cf.template_name = ct.template_name
    AND cf.status = ct.status
  WHERE ct.table_name IS NULL
  
  UNION ALL
  
  SELECT 
    'REMOVED' as change_type,
    ct.table_name,
    ct.schema_name,
    ct.template_name
  FROM {{ source('bronze_core', 'config_table') }} ct
  LEFT JOIN {{ source('bronze_core', 'config_file') }} cf
    ON ct.table_name = cf.table_name 
    AND ct.schema_name = cf.schema_name
    AND ct.template_name = cf.template_name
    AND ct.status = cf.status
  WHERE cf.table_name IS NULL
)

SELECT 
  change_type,
  COUNT(*) as change_count,
  COLLECT_SET(table_name) as affected_tables,
  COLLECT_SET(template_name) as affected_templates,
  current_timestamp() as analysis_timestamp
FROM config_changes
GROUP BY change_type

UNION ALL

SELECT 
  'EXECUTION_SUMMARY' as change_type,
  1 as change_count,
  ARRAY('Config updates completed successfully') as affected_tables,
  ARRAY('Check logs for details') as affected_templates,
  current_timestamp() as analysis_timestamp