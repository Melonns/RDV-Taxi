-- FACT_TRIPS: Central fact table untuk taxi trips dengan detailed metrics
-- Join tlc_cleaned dengan dimensions (time, location, weather)
-- Prerequisites:
--   1. Create dim_time, dim_location, dim_weather first
--   2. Weather data must be loaded to DuckDB

DROP TABLE IF EXISTS fact_trips;

CREATE TABLE fact_trips AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY t.pickup_datetime) as trip_id,
    
    -- Dimension keys (foreign keys)
    dt.time_key,
    dl_pickup.location_key as pickup_location_key,
    dl_dropoff.location_key as dropoff_location_key,
    COALESCE(dw.weather_key, -1) as weather_key,
    
    -- Trip identifiers
    CAST(t.rate_code_id AS INTEGER) as rate_code_id,
    t.store_and_fwd_flag,
    
    -- Time details
    t.pickup_datetime,
    t.dropoff_datetime,
    CAST(t.hour_of_day AS INTEGER) as hour_of_day,
    t.day_of_week,
    t.is_weekend,
    t.is_peak_hour,
    
    -- Trip metrics
    t.trip_distance,
    t.trip_duration_min,
    CAST(t.passenger_count AS INTEGER) as passenger_count,
    
    -- Financial metrics
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.total_amount,
    t.tip_percentage,
    
    -- Location coordinates
    t.pickup_longitude,
    t.pickup_latitude,
    t.dropoff_longitude,
    t.dropoff_latitude,
    
    -- Weather conditions at pickup time
    COALESCE(dw.temperature_2m, -999.0) as temperature_2m,
    COALESCE(dw.relative_humidity_2m, -1.0) as relative_humidity_2m,
    COALESCE(dw.precipitation, 0.0) as precipitation,
    COALESCE(dw.wind_speed_10m, -1.0) as wind_speed_10m,
    COALESCE(dw.weather_description, 'Unknown') as weather_description,
    COALESCE(dw.temperature_category, 'Unknown') as temperature_category,
    
    -- Data quality & categorization
    CURRENT_TIMESTAMP as loaded_at,
    CASE 
        WHEN t.trip_duration_min < 1 THEN 'Very short'
        WHEN t.trip_duration_min < 5 THEN 'Short'
        WHEN t.trip_duration_min < 15 THEN 'Medium'
        WHEN t.trip_duration_min < 30 THEN 'Long'
        ELSE 'Very long'
    END as trip_duration_category,
    
    CASE 
        WHEN t.fare_amount = 0 THEN 'No charge'
        WHEN t.tip_percentage IS NULL OR t.tip_percentage = 0 THEN 'No tip'
        WHEN t.tip_percentage < 10 THEN 'Low tip'
        WHEN t.tip_percentage < 20 THEN 'Standard tip'
        ELSE 'High tip'
    END as tip_category
    
FROM tlc_cleaned t
LEFT JOIN dim_time dt 
    ON CAST(DATE_TRUNC('day', t.pickup_datetime) AS DATE) = dt.date_key
LEFT JOIN dim_location dl_pickup 
    ON t.pickup_location_id = dl_pickup.location_id
LEFT JOIN dim_location dl_dropoff 
    ON t.dropoff_location_id = dl_dropoff.location_id
LEFT JOIN dim_weather dw 
    ON CAST(DATE_TRUNC('day', t.pickup_datetime) AS DATE) = dw.date_key
    
WHERE t.pickup_location_id IS NOT NULL
    AND t.dropoff_location_id IS NOT NULL
    
ORDER BY t.pickup_datetime;
