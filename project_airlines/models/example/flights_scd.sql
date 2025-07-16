{{ config(
    materialized='incremental',
    unique_key='flight_id',
    incremental_strategy='merge',
    schema='silver'
) }}

{% if not is_incremental() %}

SELECT
    flight_id,
    airline,
    origin,
    destination,
    flight_date,
    _rescued_data,
    created_at AS effective_from,
    '9999-12-31 23:59:59' AS effective_to,
    'Y' AS is_current
FROM {{ ref('flights_silver') }}

{% else %}

WITH staged_flights AS (
    SELECT
        flight_id,
        airline,
        origin,
        destination,
        flight_date,
        _rescued_data,
        created_at,
        ROW_NUMBER() OVER (
            PARTITION BY flight_id
            ORDER BY created_at DESC
        ) AS row_num
    FROM {{ ref('flights_silver') }}
),

latest_flights AS (
    SELECT *
    FROM staged_flights
    WHERE row_num = 1
),

changed_records AS (
    SELECT
        tgt.flight_id AS existing_flight_id,
        tgt.airline AS existing_airline,
        tgt.origin AS existing_origin,
        tgt.destination AS existing_destination,
        tgt.flight_date AS existing_flight_date,
        tgt._rescued_data AS existing__rescued_data,
        tgt.effective_from,
        tgt.effective_to,
        tgt.is_current,
        src.flight_id AS new_flight_id,
        src.airline AS new_airline,
        src.origin AS new_origin,
        src.destination AS new_destination,
        src.flight_date AS new_flight_date,
        src._rescued_data AS new__rescued_data,
        src.created_at AS new_effective_from
    FROM latest_flights src
    LEFT JOIN {{ this }} tgt
      ON src.flight_id = tgt.flight_id
      AND tgt.is_current = 'Y'
    WHERE
      tgt.flight_id IS NULL
      OR tgt.airline <> src.airline
      OR tgt.origin <> src.origin
      OR tgt.destination <> src.destination
      OR tgt.flight_date <> src.flight_date
      OR tgt._rescued_data <> src._rescued_data
),

records_to_close AS (
    SELECT
        existing_flight_id AS flight_id,
        existing_airline AS airline,
        existing_origin AS origin,
        existing_destination AS destination,
        existing_flight_date AS flight_date,
        existing__rescued_data AS _rescued_data,
        effective_from,
        new_effective_from - INTERVAL 1 SECOND AS effective_to,
        'N' AS is_current
    FROM changed_records
    WHERE existing_flight_id IS NOT NULL
),

new_current_records AS (
    SELECT
        new_flight_id AS flight_id,
        new_airline AS airline,
        new_origin AS origin,
        new_destination AS destination,
        new_flight_date AS flight_date,
        new__rescued_data AS _rescued_data,
        new_effective_from AS effective_from,
        '9999-12-31 23:59:59' AS effective_to,
        'Y' AS is_current
    FROM changed_records
),

unchanged_records AS (
    SELECT *
    FROM {{ this }}
    WHERE is_current = 'Y'
      AND flight_id NOT IN (
          SELECT existing_flight_id FROM changed_records
      )
)

SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM records_to_close
UNION ALL
SELECT * FROM new_current_records


{% endif %}