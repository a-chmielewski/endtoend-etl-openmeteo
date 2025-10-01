SELECT
    city,
    timestamp,
    CAST(temperature_2m AS DOUBLE PRECISION) AS temperature_2m,
    CAST(precipitation AS DOUBLE PRECISION) AS precipitation,
    CAST(wind_speed_10m AS DOUBLE PRECISION) AS wind_speed_10m
FROM {{ source('staging', 'weather_hourly') }}