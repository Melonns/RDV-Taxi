-- DIM_WEATHER: Dimension table untuk weather conditions
-- Extract dari weather_transformed table di DuckDB (daily)

DROP TABLE IF EXISTS dim_weather;

CREATE TABLE dim_weather AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY date_col) as weather_key,
    date_col as date_key,
    CAST(avg_temperature AS FLOAT) as temperature_2m,
    CAST(avg_humidity AS FLOAT) as relative_humidity_2m,
    CAST(total_precipitation AS FLOAT) as precipitation,
    CAST(NULL AS FLOAT) as wind_speed_10m,
    CAST(NULL AS FLOAT) as wind_direction_10m,
    CAST(NULL AS INTEGER) as weather_code,
    COALESCE(temperature_category, 'Unknown') as weather_description,
    CASE 
        WHEN CAST(avg_temperature AS FLOAT) < 0 THEN 'Freezing'
        WHEN CAST(avg_temperature AS FLOAT) < 10 THEN 'Cold'
        WHEN CAST(avg_temperature AS FLOAT) < 20 THEN 'Cool'
        WHEN CAST(avg_temperature AS FLOAT) < 30 THEN 'Warm'
        ELSE 'Hot'
    END as temperature_category,
    CURRENT_TIMESTAMP as loaded_at
FROM (
    SELECT 
        CAST(date_actual AS DATE) as date_col,
        avg_temperature,
        min_temperature,
        max_temperature,
        avg_humidity,
        total_precipitation,
        temperature_category
    FROM weather_transformed
)
ORDER BY date_col;
