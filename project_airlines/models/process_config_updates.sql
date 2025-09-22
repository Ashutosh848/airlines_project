{{ config(
    materialized='table',
    pre_hook="{{ log('🚀 Starting config update process for bronze.core...', info=true) }}",
    post_hook=[
      "{{ update_helper_table() }}",
      "{{ drop_removed_tables() }}", 
      "{{ drop_added_template_tables() }}",
      "{{ log('✅ Finished config update process.', info=true) }}"
    ]
) }}

-- Compare config_file vs config_table and analyze changes
WITH config_changes AS (
  -- Records in config_file but not in config_table (ADDED)
  SELECT 
    'ADDED' as change_type,
    cf.table_name,
    cf.schema_name,
    cf.template_name,
    cf.status,
    cf.last_updated,
    NULL as old_template_name,
    NULL as old_status
  FROM {{ source('bronze_core', 'config_file') }} cf
  LEFT JOIN {{ source('bronze_core', 'config_table') }} ct 
    ON cf.table_name = ct.table_name 
    AND cf.schema_name = ct.schema_name
    AND cf.template_name = ct.template_name
    AND cf.status = ct.status
  WHERE ct.table_name IS NULL
  
  UNION ALL
  
  -- Records in config_table but not in config_file (REMOVED)
  SELECT 
    'REMOVED' as change_type,
    ct.table_name,
    ct.schema_name,
    ct.template_name,
    ct.status,
    ct.last_updated,
    ct.template_name as old_template_name,
    ct.status as old_status
  FROM {{ source('bronze_core', 'config_table') }} ct
  LEFT JOIN {{ source('bronze_core', 'config_file') }} cf
    ON ct.table_name = cf.table_name 
    AND ct.schema_name = cf.schema_name
    AND ct.template_name = cf.template_name
    AND ct.status = cf.status
  WHERE cf.table_name IS NULL
),

helper_impact AS (
  -- Show which helper_table records will be affected
  SELECT 
    h.table_name,
    h.schema_name,
    h.identified_template_name,
    h.processing_status,
    CASE 
      WHEN cf.table_name IS NOT NULL THEN 'MATCHED_IN_CONFIG_FILE'
      WHEN cf.table_name IS NULL THEN 'NOT_IN_CONFIG_FILE' 
    END as helper_status
  FROM {{ source('bronze_core', 'helper_table') }} h
  LEFT JOIN {{ source('bronze_core', 'config_file') }} cf
    ON h.table_name = cf.table_name
    AND h.schema_name = cf.schema_name
    AND LOWER(COALESCE(h.identified_template_name, '')) = LOWER(COALESCE(cf.template_name, ''))
  WHERE h.identified_template_name IS NOT NULL
)

-- Final output showing all changes and their impact
SELECT 
  cc.change_type,
  COUNT(*) as change_count,
  COLLECT_SET(cc.table_name) as affected_tables,
  COLLECT_SET(cc.template_name) as affected_templates,
  current_timestamp() as analysis_timestamp
FROM config_changes cc
GROUP BY cc.change_type

UNION ALL

SELECT 
  'HELPER_IMPACT' as change_type,
  COUNT(*) as change_count,
  COLLECT_SET(hi.table_name) as affected_tables,
  COLLECT_SET(hi.identified_template_name) as affected_templates,
  current_timestamp() as analysis_timestamp
FROM helper_impact hi
WHERE hi.helper_status = 'MATCHED_IN_CONFIG_FILE'