-- models/airports_cleaned.sql
{{ config(
    schema='silver', 
    materialized='incremental', 
    incremental_strategy='merge',
    unique_key = 'booking_id'
) }}


SELECT 
    booking_id,
    passenger_id,
    flight_id,
    airport_id,
    CAST(amount as FLOAT) as amount,
    CAST(booking_date as date) as booking_date,
    _rescued_data,
    updated_at
FROM {{ source('bronze', 'bookings_raw') }}
{% if is_incremental() %}
  WHERE updated_at > (
    SELECT max(updated_at)
    FROM {{ this }}
  )
{% endif %}