-- models/airports_cleaned.sql
{{ config(
    schema='silver', 
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'airport_id'
)}}


SELECT 
    CAST(airport_id as VARCHAR(100)) as airport_id,
    airport_name, city, country, _rescued_data, updated_at
FROM {{ source('bronze', 'airports_raw') }}
{% if is_incremental() %}
  WHERE updated_at > (
    SELECT max(updated_at)
    FROM {{ this }}
  )
{% endif %}