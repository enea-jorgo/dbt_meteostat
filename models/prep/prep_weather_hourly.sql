WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
        , timestamp::DATE AS date -- Extract the date part from timestamp
        , timestamp::TIME AS time -- Extract the time part from timestamp
        , TO_CHAR(timestamp,'HH24:MI') as hour -- Extract time in HH:MM format as text
        , TO_CHAR(timestamp, 'FMmonth') AS month_name -- Month name as a text
        , DATE_PART ('day', timestamp) AS date_day -- Day of the month
        , DATE_PART ('month', timestamp) AS date_month -- Numeric month
        , DATE_PART ('year', timestamp)  AS date_year -- Numeric year
        , DATE_PART ('week', timestamp)  AS cw -- Week number
        , TO_CHAR(timestamp, 'Month')  AS month_full_name -- Full month name
        , TO_CHAR(timestamp,'Day')  AS weekday -- Day name
    FROM hourly_data
),
add_more_features AS (
    SELECT *
        , (CASE 
            WHEN time BETWEEN TIME '00:00:00' AND TIME '06:00:00' THEN 'night'
            WHEN time BETWEEN TIME '06:00:00' AND TIME '12:00:00' THEN 'morning'
            WHEN time BETWEEN TIME '12:00:00' AND TIME '18:00:00' THEN 'afternoon'
            ELSE 'evening'
          END) AS day_part -- Categorize time into day parts
    FROM add_features
)

SELECT *
FROM add_more_features
