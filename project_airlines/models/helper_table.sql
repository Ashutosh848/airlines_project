{{ config(
    materialized='table',
    schema='core',
    database='bronze',
    post_hook="DELETE FROM " ~ source('bronze_core', 'helper_table') ~ " WHERE processing_status IN ('FAILED', 'PENDING_PROCESSING')"
) }}

-- the model itself can be a simple query, just to satisfy dbt
SELECT * FROM {{ source('bronze_core', 'helper_table') }}
