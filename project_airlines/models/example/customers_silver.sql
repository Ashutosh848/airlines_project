-- models/airports_cleaned.sql
{{ config(
    schema='silver',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'passenger_id'
) }}


SELECT *
FROM {{ source('bronze', 'customers_raw') }}
{% if is_incremental() %}
  WHERE updated_at > (
    SELECT max(updated_at)
    FROM {{ this }}
  )
{% endif %}