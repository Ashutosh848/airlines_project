{% macro drop_added_template_tables() %}
  {% set added_templates_sql %}
    SELECT DISTINCT 
      cf.template_name,
      cf.schema_name
    FROM bronze.core.config_file cf
    LEFT JOIN bronze.core.config_table ct 
      ON cf.table_name = ct.table_name
      AND cf.schema_name = ct.schema_name
      AND cf.template_name = ct.template_name
      AND cf.status = ct.status
    WHERE ct.table_name IS NULL
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(added_templates_sql) %}
    {% set dropped_count = 0 %}
    {% if results %}
      {% for row in results %}
        {% set template_prefix = row[0] | lower %}
        {% set schema_name = 'bronze.ein_' ~ row[1] ~ '_silver' %}
        
        -- Check if schema exists first
        {% set schema_exists_sql = "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '" ~ row[1] ~ "_silver'" %}
        {% set schema_result = run_query(schema_exists_sql) %}
        
        {% if schema_result and schema_result.columns[0].values()[0] > 0 %}
          -- Get all tables that start with template prefix
          {% set show_tables_sql = "SHOW TABLES IN " ~ schema_name %}
          {% set table_results = run_query(show_tables_sql) %}
          
          {% if table_results %}
            {% for table_row in table_results %}
              {% set table_name = table_row[1] %}
              {% if table_name.lower().startswith(template_prefix) %}
                {% set full_table = schema_name ~ '.' ~ table_name %}
                {% do run_query("DROP TABLE IF EXISTS " ~ full_table) %}
                {% do log("🗑️ Dropped template table: " ~ full_table, info=true) %}
                {% set dropped_count = dropped_count + 1 %}
              {% endif %}
            {% endfor %}
          {% endif %}
        {% endif %}
      {% endfor %}
    {% endif %}
    {% do log("📊 Total template tables dropped for cleanup: " ~ dropped_count, info=true) %}
  {% endif %}
{% endmacro %}
