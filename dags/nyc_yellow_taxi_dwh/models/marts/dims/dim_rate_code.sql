{{
  config(
    materialized='table'
  )
}}

WITH rate_code_data AS (
    SELECT DISTINCT
        ratecodeid_clean as ratecodeid,
        rate_code_description
    FROM {{ source('silver_data', 'yellow_tripdata_2024') }}
    WHERE ratecodeid IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ratecodeid']) }} as rate_code_key,
    ratecodeid,
    rate_code_description
FROM rate_code_data