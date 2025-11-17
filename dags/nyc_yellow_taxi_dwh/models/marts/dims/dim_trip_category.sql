{{
  config(
    materialized='table'
  )
}}

WITH trip_category_data AS (
    SELECT DISTINCT
        distance_category,
        duration_category,
        fare_category
    FROM {{ source('silver_data', 'yellow_tripdata_2024') }}
    WHERE distance_category IS NOT NULL 
       OR duration_category IS NOT NULL 
       OR fare_category IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['distance_category', 'duration_category', 'fare_category']) }} as trip_category_key,
    distance_category,
    duration_category,
    fare_category
FROM trip_category_data