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
    flight_date,
    _rescued_data,
    updated_at
FROM {{ source('bronze', 'flights_raw') }}

{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
