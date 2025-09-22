{% macro drop_removed_tables() %}
  {% set removed_tables_sql %}
    SELECT DISTINCT
      h.table_name,
      h.schema_name, 
      h.identified_template_name
    FROM bronze.core.helper_table h
    LEFT JOIN bronze.core.config_file cf 
      ON h.table_name = cf.table_name 
      AND h.schema_name = cf.schema_name
      AND LOWER(COALESCE(h.identified_template_name, '')) = LOWER(COALESCE(cf.template_name, ''))
    WHERE cf.table_name IS NULL 
      AND h.identified_template_name IS NOT NULL
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(removed_tables_sql) %}
    {% set dropped_count = 0 %}
    {% if results %}
      {% for row in results %}
        {% if row[2] %} -- identified_template_name exists
          {% set full_table = 'bronze.ein_' ~ row[1] ~ '_silver.' ~ row[2] %}
          {% do run_query("DROP TABLE IF EXISTS " ~ full_table) %}
          {% do log("🗑️ Dropped removed table: " ~ full_table, info=true) %}
          {% set dropped_count = dropped_count + 1 %}
        {% endif %}
      {% endfor %}
    {% endif %}
    {% do log("Total tables dropped for removed configs: " ~ dropped_count, info=true) %}
  {% endif %}
{% endmacro %}
