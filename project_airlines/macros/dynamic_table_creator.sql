{% macro dynamic_table_creator() %}

{% set source_query %}
SELECT json_data 
FROM {{ source('bronze_core', 'json_table') }}

{% endset %}

{% set results = run_query(source_query) %}

{% if execute %}
{% for row in results %}
    {% if row[0] is not none %}
        {% set parsed_json = fromjson(row[0]) %}
        
        {% for table_obj in parsed_json.tables %}
            {% set table_name = table_obj.name %}
            {% set table_data = table_obj.data %}
            
            {% if table_data and table_data | length > 0 %}
                
                {% set first_record = table_data[0] %}
                {% set columns = first_record.keys() %}
                
                -- Create the table dynamically
                {% set create_table_query %}
CREATE OR REPLACE TABLE bronze.core.{{ table_name }} (
    {% for column in columns %}
        {{ column }} STRING{% if not loop.last %},{% endif %}
    {% endfor %}
)
                {% endset %}
                
                -- Execute table creation
                {% do run_query(create_table_query) %}
                {{ log("Created table: bronze.core." ~ table_name, info=true) }}
                
                -- Insert data into the table (FIXED VERSION)
                {% set insert_values = [] %}
                {% for record in table_data %}
                    {% set record_values = [] %}
                    {% for column in columns %}
                        {% set value = record[column] %}
                        {% if value is none or value == "null" or value == "NULL" %}
                            {% do record_values.append("NULL") %}
                        {% else %}
                            {# Properly escape single quotes by doubling them #}
                            {% set escaped_value = value | replace("'", "''") %}
                            {% do record_values.append("'" ~ escaped_value ~ "'") %}
                        {% endif %}
                    {% endfor %}
                    {% do insert_values.append("(" ~ record_values | join(", ") ~ ")") %}
                {% endfor %}
                
                {% set insert_query %}
INSERT INTO bronze.core.{{ table_name }} 
VALUES 
{{ insert_values | join(",\n") }}
                {% endset %}
                
                -- Execute data insertion
                {% do run_query(insert_query) %}
                {{ log("Inserted " ~ table_data | length ~ " records into bronze.core." ~ table_name, info=true) }}
                
            {% endif %}
        {% endfor %}
    {% endif %}
{% endfor %}
{% endif %}

{% endmacro %}