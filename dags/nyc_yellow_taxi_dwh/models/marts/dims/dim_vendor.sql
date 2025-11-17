{{
  config(
    materialized='table'
  )
}}

WITH vendor_data AS (
    SELECT DISTINCT
        vendorid,
        vendor_name
    FROM {{ source('silver_data', 'yellow_tripdata_2024') }}
    WHERE vendorid IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['vendorid']) }} as vendor_key,
    vendorid,
    vendor_name
FROM vendor_data