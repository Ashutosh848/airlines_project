-- models/airports_cleaned.sql
{{ config(schema='bronze', materialized='table') }}


SELECT * 
FROM {{ source('bronze', 'airports_raw') }};

{% if is_incremental %}
  WHERE updated_at>(SELECT max(updated_at) from {{ this }})
{% endif %}