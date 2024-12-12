SELECT
    pws.date
    ,mfs.*
    ,pws.min_temp_c AS min_temp
    ,pws.max_temp_c AS max_temp
    ,pws.precipitation_mm AS precipitation
    ,pws.max_snow_mm AS snow_fall
    ,pws.avg_wind_direction
    ,pws.avg_wind_speed_kmh AS avg_wind_speed
    ,pws.wind_peakgust_kmh AS wind_peakgust
FROM {{ref('mart_faa_stats')}} mfs
JOIN {{ref('prep_weather_daily')}} pws
  ON mfs.faa = pws.airport_code
ORDER BY faa, date