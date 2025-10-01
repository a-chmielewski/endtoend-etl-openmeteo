CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.weather_hourly (
    city TEXT,
    timestamp TIMESTAMPTZ,
    temperature_2m DOUBLE PRECISION,
    precipitation DOUBLE PRECISION,
    wind_speed_10m DOUBLE PRECISION,
    _ingested_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (city, timestamp)
)