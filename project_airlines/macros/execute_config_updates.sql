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
          {% set catalog_name = 'bronze' %}
          {% set schema_db_name = 'ein_' ~ schema_name ~ '_silver' %}
          
          {# Check if table exists in Databricks Unity Catalog #}
          {% set check_table_exists_sql %}
            SELECT COUNT(*) as table_exists 
            FROM system.information_schema.tables 
            WHERE table_catalog = '{{ catalog_name }}'
              AND table_schema = '{{ schema_db_name }}'
              AND table_name = '{{ template_name }}'
          {% endset %}
          
          {% set exists_result = run_query(check_table_exists_sql) %}
          {% if exists_result and exists_result.rows[0][0] > 0 %}
            {% set drop_sql = "DROP TABLE IF EXISTS " ~ full_table %}
            {% do run_query(drop_sql) %}
            {% do log("Dropped table: " ~ full_table, info=true) %}
            {% set dropped_count = dropped_count + 1 %}
          {% else %}
            {% do log("Table does not exist, skipping drop: " ~ full_table, info=true) %}
          {% endif %}
        {% endif %}
      {% endfor %}
    {% endif %}
    {% do log("Total tables dropped: " ~ dropped_count, info=true) %}
  {% endif %}
  
  {% do log("Finished config update process.", info=true) %}
{% endmacro %}