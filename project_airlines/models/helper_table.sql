{{
  config(
    materialized='table'
  )
}}

SELECT * FROM {{ source('bronze_core', 'helper_table') }}
