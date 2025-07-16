-- models/airports_cleaned.sql
{{ config(
    schema='silver',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'passenger_id'
) }}


SELECT 
  passenger_id,
  name,
  gender,
  nationality,
  _rescued_data,
  updated_at as created_at
FROM {{ source('bronze', 'customers_raw') }}
{% if is_incremental() %}
  WHERE created_at > (
    SELECT max(created_at)
    FROM {{ this }}
  )
{% endif %}