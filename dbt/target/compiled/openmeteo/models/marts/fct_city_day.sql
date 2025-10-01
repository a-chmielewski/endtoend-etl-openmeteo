
WITH h AS (
    SELECT city, DATE_TRUNC('day', timestamp) AS day,
           AVG(temperature_2m) AS temperature_2m,
           AVG(precipitation) AS precipitation,
           AVG(wind_speed_10m) AS wind_speed_10m
    FROM "analytics"."staging"."weather_hourly"
    GROUP BY 1, 2
)
SELECT city, day, temperature_2m, precipitation, wind_speed_10m
FROM h