{% macro update_helper_table() %}
  {% set update_sql %}
    UPDATE bronze.core.helper_table 
    SET identified_template_name = NULL,
        updated_at = current_timestamp()
    WHERE EXISTS (
        SELECT 1 
        FROM bronze.core.config_file cf
        WHERE bronze.core.helper_table.table_name = cf.table_name
          AND bronze.core.helper_table.schema_name = cf.schema_name
          AND LOWER(COALESCE(bronze.core.helper_table.identified_template_name, '')) = LOWER(COALESCE(cf.template_name, ''))
    )
  {% endset %}
  
  {% if execute %}
    {% do run_query(update_sql) %}
    {% do log("🔄 Updated helper_table - nullified " ~ run_query("SELECT COUNT(*) FROM bronze.core.helper_table WHERE identified_template_name IS NULL AND updated_at >= current_date()").columns[0].values()[0] ~ " template names", info=true) %}
  {% endif %}
{% endmacro %}
