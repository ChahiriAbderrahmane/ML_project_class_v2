{{
  config(
    materialized='table'
  )
}}

WITH store_forward_data AS (
    SELECT DISTINCT
        store_and_fwd_flag_cleaned as store_and_fwd_flag,
        store_and_fwd_description
    FROM {{ source('silver_data', 'yellow_tripdata_2024') }}
    WHERE store_and_fwd_flag IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['store_and_fwd_flag']) }} as store_forward_key,
    store_and_fwd_flag,
    store_and_fwd_description
FROM store_forward_data