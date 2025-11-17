{{ config(materialized='table') }}

WITH all_datetimes AS (
    SELECT DISTINCT
        tpep_pickup_datetime AS full_datetime
    FROM {{ source('silver_data', 'yellow_tripdata_2024') }}
    WHERE tpep_pickup_datetime IS NOT NULL

    UNION

    SELECT DISTINCT
        tpep_dropoff_datetime AS full_datetime
    FROM {{ source('silver_data', 'yellow_tripdata_2024') }}
    WHERE tpep_dropoff_datetime IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['full_datetime']) }} AS datetime_key,
    full_datetime,
    DATE(full_datetime) AS date,
    EXTRACT(YEAR FROM full_datetime) AS year,
    EXTRACT(MONTH FROM full_datetime) AS month,
    EXTRACT(DAY FROM full_datetime) AS day,
    EXTRACT(HOUR FROM full_datetime) AS hour,
    EXTRACT(MINUTE FROM full_datetime) AS minute,
    EXTRACT(SECOND FROM full_datetime) AS second,

    EXTRACT(ISODOW FROM full_datetime) AS day_of_week_num,
    TRIM(TO_CHAR(full_datetime, 'Day')) AS day_of_week_name,

    EXTRACT(WEEK FROM full_datetime) AS week_of_year,
    EXTRACT(QUARTER FROM full_datetime) AS quarter,

    CASE 
        WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN EXTRACT(HOUR FROM full_datetime) BETWEEN 18 AND 22 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,

    CASE 
        WHEN EXTRACT(ISODOW FROM full_datetime) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type

FROM all_datetimes
