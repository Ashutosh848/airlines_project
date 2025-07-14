-- models/airports_cleaned.sql
{{ config(schema='bronze', materialized='incremental') }}


SELECT *
FROM {{ source('bronze', 'bookings_raw') }}
{% if is_incremental() %}
  WHERE updated_at > (
    SELECT max(updated_at)
    FROM {{ this }}
  )
{% endif %}