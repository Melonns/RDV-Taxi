-- DIM_WEATHER: Dimension table untuk weather conditions
-- Extract dari weather transformed data (daily)

DROP TABLE IF EXISTS dim_weather;

CREATE TABLE dim_weather AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY date_col) as weather_key,
    date_col as date_key,
    CAST(temperature_2m_mean AS FLOAT) as temperature_2m,
    CAST(relative_humidity_2m_mean AS FLOAT) as relative_humidity_2m,
    CAST(precipitation_sum AS FLOAT) as precipitation,
    CAST(wind_speed_10m_mean AS FLOAT) as wind_speed_10m,
    CAST(wind_direction_10m_mean AS FLOAT) as wind_direction_10m,
    CAST(weathercode AS INTEGER) as weather_code,
    COALESCE(weather_category, 'Unknown') as weather_description,
    CASE 
        WHEN CAST(temperature_2m_mean AS FLOAT) < 0 THEN 'Freezing'
        WHEN CAST(temperature_2m_mean AS FLOAT) < 10 THEN 'Cold'
        WHEN CAST(temperature_2m_mean AS FLOAT) < 20 THEN 'Cool'
        WHEN CAST(temperature_2m_mean AS FLOAT) < 30 THEN 'Warm'
        ELSE 'Hot'
    END as temperature_category,
    CURRENT_TIMESTAMP as loaded_at
FROM (
    SELECT 
        CAST(date AS DATE) as date_col,
        temperature_2m_mean,
        temperature_2m_min,
        temperature_2m_max,
        relative_humidity_2m_mean,
        precipitation_sum,
        wind_speed_10m_mean,
        wind_direction_10m_mean,
        weathercode,
        weather_category
    FROM read_parquet('data/intermediate/weather/weather_daily_transformed.parquet')
)
ORDER BY date_col;
