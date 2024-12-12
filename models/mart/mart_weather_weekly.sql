SELECT
    airport_code
    ,cw
    ,round(avg(avg_temp_c), 2) AS avg_temp_w
    ,min(min_temp_c) AS min_temp_w
    ,max(max_temp_c) AS max_temp_w
    ,round(avg(precipitation_mm),2) AS avg_precipitation_w
    ,max(max_snow_mm) AS avg_snow_w
    ,round(avg(avg_wind_direction),2) AS avg_wind_direction_w
    ,round(avg(avg_wind_speed_kmh),2) AS avg_wind_speed_w
    ,max(wind_peakgust_kmh) AS max_wind_peakgust_w
    ,round(avg(avg_pressure_hpa),2) AS avg_pressure_w
    ,sum(sun_minutes) AS sum_sun_minutes_w
FROM {{ref('prep_weather_daily')}} pwd
GROUP BY airport_code, cw
ORDER BY airport_code, cw