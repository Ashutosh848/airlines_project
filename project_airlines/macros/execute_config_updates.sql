{% macro execute_config_updates() %}
  {% do log("Starting config update process...", info=true) %}
  
  {% set removed_tables_sql %}
    SELECT DISTINCT
      ct.table_name,
      ct.schema_name, 
      ct.template_name
    FROM bronze.core.config_table ct
    LEFT JOIN bronze.core.config_file cf 
      ON ct.table_name = cf.table_name 
      AND ct.schema_name = cf.schema_name
      AND ct.template_name = cf.template_name
      AND ct.status = cf.status
    WHERE cf.table_name IS NULL 
      AND ct.template_name IS NOT NULL
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(removed_tables_sql) %}
    {% set dropped_count = 0 %}
    {% if results.rows %}
      {% for row in results.rows %}
        {% set table_name = row[0] %}
        {% set schema_name = row[1] %}
        {% set template_name = row[2] %}
        {% if template_name %}
          {% set full_table = 'bronze.ein_' ~ schema_name ~ '_silver.' ~ template_name %}
          {% set drop_sql = "DROP TABLE IF EXISTS " ~ full_table %}
          {% do run_query(drop_sql) %}
          {% do log("Dropped table: " ~ full_table, info=true) %}
          {% set dropped_count = dropped_count + 1 %}
        {% endif %}
      {% endfor %}
    {% endif %}
    {% do log("Total tables dropped: " ~ dropped_count, info=true) %}
  {% endif %}
  
  {% do log("Finished config update process.", info=true) %}
{% endmacro %}