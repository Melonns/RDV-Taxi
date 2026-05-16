/*
Kelompok Proyek Rekayasa Data:
1. 235150201111036 DARVESH AZIZ MAWLA
2. 235150207111063 ACHMAD ALVIAN PRASETIO
3. 235150207111006 DZAKY REZANDI
4. 235150201111004 WAHYU DWI LAKSANA PUTRI
5. 235150207111065 JONATHAN SALIM
*/

-- RDV Taxi Weather Analysis
-- Period: January - June 2025
-- Outlier handling:
-- tip_percentage difilter antara 0 dan 100


-- 1. Dataset overview setelah difilter
SELECT
    COUNT(*) AS total_trips,
    MIN(pickup_datetime) AS start_date,
    MAX(pickup_datetime) AS end_date,
    AVG(tip_percentage) AS avg_tip_percentage,
    AVG(trip_duration_min) AS avg_trip_duration_min,
    AVG(trip_distance) AS avg_trip_distance,
    SUM(total_amount) AS total_revenue
FROM fact_trips
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime < DATE '2025-07-01'
  AND tip_percentage BETWEEN 0 AND 100;


-- 2. Weather impact on tip and duration
SELECT
    weather_description,
    COUNT(*) AS total_trips,
    AVG(tip_percentage) AS avg_tip_percentage,
    AVG(trip_duration_min) AS avg_trip_duration_min,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(total_amount) AS avg_total_amount
FROM fact_trips
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime < DATE '2025-07-01'
  AND tip_percentage BETWEEN 0 AND 100
GROUP BY weather_description
ORDER BY avg_tip_percentage DESC;


-- 3. Temperature category impact
SELECT
    temperature_category,
    COUNT(*) AS total_trips,
    AVG(temperature_2m) AS avg_temperature,
    AVG(tip_percentage) AS avg_tip_percentage,
    AVG(trip_duration_min) AS avg_trip_duration_min
FROM fact_trips
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime < DATE '2025-07-01'
  AND tip_percentage BETWEEN 0 AND 100
GROUP BY temperature_category
ORDER BY avg_temperature;


-- 4. Tip percentage by hour and weather
SELECT
    hour_of_day,
    weather_description,
    COUNT(*) AS total_trips,
    AVG(tip_percentage) AS avg_tip_percentage
FROM fact_trips
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime < DATE '2025-07-01'
  AND tip_percentage BETWEEN 0 AND 100
GROUP BY hour_of_day, weather_description
ORDER BY hour_of_day, weather_description;


-- 5. Trip duration by hour and weather
SELECT
    hour_of_day,
    weather_description,
    COUNT(*) AS total_trips,
    AVG(trip_duration_min) AS avg_trip_duration_min
FROM fact_trips
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime < DATE '2025-07-01'
  AND tip_percentage BETWEEN 0 AND 100
GROUP BY hour_of_day, weather_description
ORDER BY hour_of_day, weather_description;


-- 6. Peak hour vs non-peak hour
SELECT
    is_peak_hour,
    weather_description,
    COUNT(*) AS total_trips,
    AVG(tip_percentage) AS avg_tip_percentage,
    AVG(trip_duration_min) AS avg_trip_duration_min,
    AVG(trip_distance) AS avg_trip_distance
FROM fact_trips
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime < DATE '2025-07-01'
  AND tip_percentage BETWEEN 0 AND 100
GROUP BY is_peak_hour, weather_description
ORDER BY is_peak_hour, weather_description;


-- 7. Tip category distribution
SELECT
    tip_category,
    COUNT(*) AS total_trips,
    AVG(tip_percentage) AS avg_tip_percentage,
    AVG(fare_amount) AS avg_fare_amount,
    AVG(total_amount) AS avg_total_amount
FROM fact_trips
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime < DATE '2025-07-01'
  AND tip_percentage BETWEEN 0 AND 100
GROUP BY tip_category
ORDER BY total_trips DESC;


-- 8. Duration category distribution
SELECT
    trip_duration_category,
    COUNT(*) AS total_trips,
    AVG(trip_duration_min) AS avg_trip_duration_min,
    AVG(tip_percentage) AS avg_tip_percentage
FROM fact_trips
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime < DATE '2025-07-01'
  AND tip_percentage BETWEEN 0 AND 100
GROUP BY trip_duration_category
ORDER BY avg_trip_duration_min;


-- 9. Data quality check after filtering
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN tip_percentage IS NULL THEN 1 ELSE 0 END) AS null_tip_percentage,
    SUM(CASE WHEN trip_duration_min IS NULL THEN 1 ELSE 0 END) AS null_trip_duration,
    SUM(CASE WHEN weather_description IS NULL THEN 1 ELSE 0 END) AS null_weather_description,
    MIN(tip_percentage) AS min_tip_percentage,
    MAX(tip_percentage) AS max_tip_percentage,
    MIN(trip_duration_min) AS min_trip_duration,
    MAX(trip_duration_min) AS max_trip_duration
FROM fact_trips
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime < DATE '2025-07-01'
  AND tip_percentage BETWEEN 0 AND 100;


-- 10. Outlier summary before filtering
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN tip_percentage > 100 THEN 1 ELSE 0 END) AS tip_above_100,
    SUM(CASE WHEN tip_percentage > 200 THEN 1 ELSE 0 END) AS tip_above_200,
    SUM(CASE WHEN tip_percentage > 500 THEN 1 ELSE 0 END) AS tip_above_500,
    SUM(CASE WHEN tip_percentage > 1000 THEN 1 ELSE 0 END) AS tip_above_1000,
    MAX(tip_percentage) AS max_tip_percentage
FROM fact_trips
WHERE pickup_datetime >= DATE '2025-01-01'
  AND pickup_datetime < DATE '2025-07-01';

-- 11. Trip volume and tip percentage by pickup borough and weather
SELECT
    l.borough,
    f.weather_description,
    COUNT(*) AS total_trips,
    AVG(f.tip_percentage) AS avg_tip_percentage,
    AVG(f.trip_duration_min) AS avg_trip_duration_min,
    AVG(f.trip_distance) AS avg_trip_distance,
    AVG(f.total_amount) AS avg_total_amount
FROM fact_trips f
JOIN dim_location l
    ON f.pickup_location_key = l.location_key
WHERE f.pickup_datetime >= DATE '2025-01-01'
  AND f.pickup_datetime < DATE '2025-07-01'
  AND f.tip_percentage BETWEEN 0 AND 100
GROUP BY l.borough, f.weather_description
ORDER BY l.borough, avg_tip_percentage DESC;


-- 12. Top pickup zones by trip volume
SELECT
    l.borough,
    l.zone_name,
    COUNT(*) AS total_trips,
    AVG(f.tip_percentage) AS avg_tip_percentage,
    AVG(f.trip_duration_min) AS avg_trip_duration_min,
    AVG(f.trip_distance) AS avg_trip_distance
FROM fact_trips f
JOIN dim_location l
    ON f.pickup_location_key = l.location_key
WHERE f.pickup_datetime >= DATE '2025-01-01'
  AND f.pickup_datetime < DATE '2025-07-01'
  AND f.tip_percentage BETWEEN 0 AND 100
GROUP BY l.borough, l.zone_name
ORDER BY total_trips DESC
LIMIT 25;


-- 13. Borough level summary
SELECT
    l.borough,
    COUNT(*) AS total_trips,
    AVG(f.tip_percentage) AS avg_tip_percentage,
    AVG(f.trip_duration_min) AS avg_trip_duration_min,
    AVG(f.trip_distance) AS avg_trip_distance,
    AVG(f.total_amount) AS avg_total_amount
FROM fact_trips f
JOIN dim_location l
    ON f.pickup_location_key = l.location_key
WHERE f.pickup_datetime >= DATE '2025-01-01'
  AND f.pickup_datetime < DATE '2025-07-01'
  AND f.tip_percentage BETWEEN 0 AND 100
GROUP BY l.borough
ORDER BY total_trips DESC;


-- 14. Map data by pickup zone
SELECT
    l.borough,
    l.zone_name,
    l.latitude,
    l.longitude,
    COUNT(*) AS total_trips,
    AVG(f.tip_percentage) AS avg_tip_percentage,
    AVG(f.trip_duration_min) AS avg_trip_duration_min
FROM fact_trips f
JOIN dim_location l
    ON f.pickup_location_key = l.location_key
WHERE f.pickup_datetime >= DATE '2025-01-01'
  AND f.pickup_datetime < DATE '2025-07-01'
  AND f.tip_percentage BETWEEN 0 AND 100
  AND l.latitude IS NOT NULL
  AND l.longitude IS NOT NULL
  AND l.latitude BETWEEN 40.4 AND 41.0
  AND l.longitude BETWEEN -74.4 AND -73.5
GROUP BY l.borough, l.zone_name, l.latitude, l.longitude
ORDER BY total_trips DESC;

