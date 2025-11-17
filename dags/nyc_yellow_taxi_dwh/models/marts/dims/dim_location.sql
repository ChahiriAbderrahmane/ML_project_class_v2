{{
  config(
    materialized='table'
  )
}}

WITH pickup_locations AS (
    SELECT DISTINCT
        pulocationid as locationid,
        pickup_borough as borough,
        pickup_zone as zone
    FROM {{ source('silver_data', 'yellow_tripdata_2024') }}
    WHERE pulocationid IS NOT NULL
),

dropoff_locations AS (
    SELECT DISTINCT
        dolocationid as locationid,
        dropoff_borough as borough,
        dropoff_zone as zone
    FROM {{ source('silver_data', 'yellow_tripdata_2024') }}
    WHERE dolocationid IS NOT NULL
),

all_locations AS (
    SELECT * FROM pickup_locations
    UNION
    SELECT * FROM dropoff_locations
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['locationid']) }} as location_key,
    locationid,
    borough,
    zone
FROM all_locations