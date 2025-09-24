{% macro process_table_extraction_changes() %}
  {% do log("Processing table extraction config changes...", info=true) %}
  
  {% set removed_tables_sql %}
    WITH removed_configs AS (
      SELECT 
        ct.table_name,
        ct.schema_name, 
        ct.template_name
      FROM {{ source('bronze_core', 'config_table') }} ct
      LEFT JOIN {{ source('bronze_core', 'config_file') }} cf 
        ON ct.table_name = cf.table_name 
        AND ct.schema_name = cf.schema_name
        AND ct.template_name = cf.template_name
      WHERE cf.table_name IS NULL 
        AND ct.template_name IS NOT NULL
    )
    SELECT * FROM removed_configs
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(removed_tables_sql) %}
    {% set dropped_count = 0 %}
    {% set templates_to_nullify = [] %}
    
    {% for row in results.rows %}
      {% set table_name = row[0] %}
      {% set schema_name = row[1] %}
      {% set template_name = row[2] %}
      
      {% if template_name %}
        {% set full_table = 'bronze.ein_' ~ schema_name ~ '_silver.' ~ template_name %}
        
        {# Check if silver table exists #}
        {% set check_sql %}
          SELECT COUNT(*) as exists_flag 
          FROM system.information_schema.tables 
          WHERE table_catalog = 'bronze'
            AND table_schema = 'ein_' || '{{ schema_name }}' || '_silver'
            AND table_name = '{{ template_name }}'
        {% endset %}
        
        {% set exists_result = run_query(check_sql) %}
        {% if exists_result and exists_result.rows[0][0] > 0 %}
          {% set drop_sql = "DROP TABLE IF EXISTS " ~ full_table %}
          {% do run_query(drop_sql) %}
          {% do log("Dropped table: " ~ full_table, info=true) %}
          {% set dropped_count = dropped_count + 1 %}
        {% else %}
          {% do log("Table does not exist, skipping: " ~ full_table, info=true) %}
        {% endif %}
        
        {# Collect template names for helper table update #}
        {% do templates_to_nullify.append(template_name) %}
      {% endif %}
    {% endfor %}
    
    {# Update helper table to NULLify identified_template_name #}
    {% if templates_to_nullify %}
      {% set update_sql %}
        UPDATE {{ source('bronze_core', 'helper_table') }} 
        SET identified_template_name = NULL,
            updated_at = current_timestamp()
        WHERE identified_template_name IN (
          {% for template in templates_to_nullify %}
            '{{ template }}'{% if not loop.last %},{% endif %}
          {% endfor %}
        )
      {% endset %}
      {% do run_query(update_sql) %}
      {% do log("Updated helper table for templates: " ~ templates_to_nullify | join(', '), info=true) %}
    {% endif %}
    
    {% do log("Processing complete. Dropped " ~ dropped_count ~ " tables, updated " ~ templates_to_nullify | length ~ " helper records", info=true) %}
  {% endif %}
{% endmacro %}