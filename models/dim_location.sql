-- DIM_LOCATION: Dimension table untuk pickup/dropoff locations
-- Extract unique locations dari TLC zones dengan coordinates

DROP TABLE IF EXISTS dim_location;

CREATE TABLE dim_location AS
WITH locations AS (
    SELECT DISTINCT
        pickup_location_id as location_id,
        pickup_latitude as latitude,
        pickup_longitude as longitude
    FROM tlc_cleaned
    WHERE pickup_location_id IS NOT NULL
        AND pickup_latitude != 0 
        AND pickup_longitude != 0
    
    UNION ALL
    
    SELECT DISTINCT
        dropoff_location_id,
        dropoff_latitude,
        dropoff_longitude
    FROM tlc_cleaned
    WHERE dropoff_location_id IS NOT NULL
        AND dropoff_latitude != 0 
        AND dropoff_longitude != 0
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY location_id) as location_key,
    location_id,
    latitude,
    longitude,
    ROUND(CAST(latitude AS FLOAT), 4) as latitude_rounded,
    ROUND(CAST(longitude AS FLOAT), 4) as longitude_rounded,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as created_at
FROM (
    SELECT DISTINCT
        location_id,
        latitude,
        longitude
    FROM locations
)
ORDER BY location_id;
