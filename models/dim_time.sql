-- DIM_TIME: Dimension table untuk temporal attributes
-- Extract unique dates dari TLC data dengan temporal details

DROP TABLE IF EXISTS dim_time;

CREATE TABLE dim_time AS
WITH time_data AS (
    SELECT DISTINCT
        DATE_TRUNC('day', pickup_datetime)::DATE as date_key,
        pickup_datetime::DATE as date_actual,
        EXTRACT(YEAR FROM pickup_datetime) as year,
        EXTRACT(MONTH FROM pickup_datetime) as month,
        EXTRACT(DAY FROM pickup_datetime) as day,
        EXTRACT(QUARTER FROM pickup_datetime) as quarter,
        EXTRACT(WEEK FROM pickup_datetime) as week_of_year,
        EXTRACT(DOW FROM pickup_datetime) as day_of_week_number,
        CASE EXTRACT(DOW FROM pickup_datetime)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            -- Note: DuckDB DOW returns 0 (Sunday) to 6 (Saturday)
        END as day_of_week_name,
        CASE WHEN EXTRACT(DOW FROM pickup_datetime) IN (0, 6) THEN true ELSE false END as is_weekend,
        CASE 
            WHEN EXTRACT(MONTH FROM pickup_datetime) IN (12, 1, 2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM pickup_datetime) IN (3, 4, 5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM pickup_datetime) IN (6, 7, 8) THEN 'Summer'
            WHEN EXTRACT(MONTH FROM pickup_datetime) IN (9, 10, 11) THEN 'Fall'
        END as season
    FROM tlc_cleaned
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY date_key) as time_key,
    date_key,
    date_actual,
    year,
    month,
    day,
    quarter,
    week_of_year,
    day_of_week_number,
    day_of_week_name,
    is_weekend,
    season
FROM time_data
ORDER BY date_key;
