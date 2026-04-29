-- DIM_WEATHER: Dimension table untuk weather conditions
-- Extract dari weather transformed data (daily)
-- NOTE: Weather data harus di-load ke DuckDB terlebih dahulu

DROP TABLE IF EXISTS dim_weather;

CREATE TABLE dim_weather AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY date_col) as weather_key,
    date_col as date_key,
    CAST(temperature_2m AS FLOAT) as temperature_2m,
    CAST(relative_humidity_2m AS FLOAT) as relative_humidity_2m,
    CAST(precipitation AS FLOAT) as precipitation,
    CAST(wind_speed_10m AS FLOAT) as wind_speed_10m,
    CAST(wind_direction_10m AS FLOAT) as wind_direction_10m,
    CAST(weather_code AS INTEGER) as weather_code,
    CASE CAST(weather_code AS INTEGER)
        WHEN 0 THEN 'Clear'
        WHEN 1 THEN 'Mainly clear'
        WHEN 2 THEN 'Partly cloudy'
        WHEN 3 THEN 'Cloudy'
        WHEN 45 THEN 'Foggy'
        WHEN 48 THEN 'Depositing rime fog'
        WHEN 51 THEN 'Light drizzle'
        WHEN 53 THEN 'Moderate drizzle'
        WHEN 55 THEN 'Dense drizzle'
        WHEN 61 THEN 'Slight rain'
        WHEN 63 THEN 'Moderate rain'
        WHEN 65 THEN 'Heavy rain'
        WHEN 71 THEN 'Slight snow'
        WHEN 73 THEN 'Moderate snow'
        WHEN 75 THEN 'Heavy snow'
        WHEN 80 THEN 'Slight rain showers'
        WHEN 81 THEN 'Moderate rain showers'
        WHEN 82 THEN 'Violent rain showers'
        WHEN 85 THEN 'Slight snow showers'
        WHEN 86 THEN 'Heavy snow showers'
        WHEN 95 THEN 'Thunderstorm'
        WHEN 96 THEN 'Thunderstorm with slight hail'
        WHEN 99 THEN 'Thunderstorm with heavy hail'
        ELSE 'Unknown'
    END as weather_description,
    CASE 
        WHEN CAST(temperature_2m AS FLOAT) < 0 THEN 'Freezing'
        WHEN CAST(temperature_2m AS FLOAT) < 10 THEN 'Cold'
        WHEN CAST(temperature_2m AS FLOAT) < 20 THEN 'Cool'
        WHEN CAST(temperature_2m AS FLOAT) < 30 THEN 'Warm'
        ELSE 'Hot'
    END as temperature_category,
    CURRENT_TIMESTAMP as loaded_at
FROM (
    SELECT 
        CAST(date AS DATE) as date_col,
        temperature_2m,
        relative_humidity_2m,
        precipitation,
        wind_speed_10m,
        wind_direction_10m,
        weather_code
    FROM read_parquet('data/intermediate/weather/weather_daily_transformed.parquet')
)
ORDER BY date_col;
