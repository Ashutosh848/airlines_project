-- models/airports_cleaned.sql
{{ config(
    schema='silver', 
    materialized='incremental', 
    incremental_strategy='merge',
    unique_key = 'flight_id'
) }}


SELECT
    flight_id,
    airline,
    origin,
    destination,
    CAST(flight_date as DATE) as flight_date,
    _rescued_data,
    updated_at
FROM {{ source('bronze', 'flights_raw') }}
{% if is_incremental() %}
  WHERE updated_at > (
    SELECT max(updated_at)
    FROM {{ this }}
  )
{% endif %}