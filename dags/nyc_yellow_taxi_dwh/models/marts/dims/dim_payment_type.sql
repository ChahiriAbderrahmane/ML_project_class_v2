{{
  config(
    materialized='table'
  )
}}

WITH payment_data AS (
    SELECT DISTINCT
        payment_type,
        payment_type_description
    FROM {{ source('silver_data', 'yellow_tripdata_2024') }}
    WHERE payment_type IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['payment_type']) }} as payment_type_key,
    payment_type,
    payment_type_description
FROM payment_data