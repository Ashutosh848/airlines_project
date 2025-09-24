{% macro ensure_schema_exists(catalog_name, schema_name) %}
  {% set check_schema_sql %}
    SHOW SCHEMAS IN `{{ catalog_name }}` LIKE '{{ schema_name }}'
  {% endset %}
  
  {% set create_schema_sql %}
    CREATE SCHEMA IF NOT EXISTS `{{ catalog_name }}`.`{{ schema_name }}`
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(check_schema_sql) %}
    
    {% if results.rows|length == 0 %}
      {% do log("Schema " ~ catalog_name ~ "." ~ schema_name ~ " does not exist. Creating it...", info=true) %}
      {% do run_query(create_schema_sql) %}
      {% do log("Schema created successfully", info=true) %}
    {% else %}
      {% do log("Schema " ~ catalog_name ~ "." ~ schema_name ~ " already exists", info=true) %}
    {% endif %}
  {% endif %}
{% endmacro %}