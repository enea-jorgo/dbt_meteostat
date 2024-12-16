{{ config(materialized='view') }}
WITH flights_one_month AS (
    SELECT *
    FROM {{source('staging_flights', 'flights_filtered')}}
    WHERE DATE_PART('month', flight_date) = 1
)
SELECT * FROM flights_one_month