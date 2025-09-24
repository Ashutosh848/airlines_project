{{
  config(
    materialized='table'
  )
}}

SELECT * FROM {{ source('bronze_core', 'config_file') }}
