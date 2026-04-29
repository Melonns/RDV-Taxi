-- DIM_LOCATION: Dimension table untuk pickup/dropoff locations
-- Generated from unique PULocationID/DOLocationIDs

DROP TABLE IF EXISTS dim_location;

CREATE TABLE dim_location AS
WITH locations AS (
    SELECT DISTINCT pickup_location_id as location_id FROM tlc_cleaned WHERE pickup_location_id IS NOT NULL
    UNION
    SELECT DISTINCT dropoff_location_id FROM tlc_cleaned WHERE dropoff_location_id IS NOT NULL
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY location_id) as location_key,
    location_id,
    CAST(NULL AS FLOAT) as latitude,
    CAST(NULL AS FLOAT) as longitude,
    CAST(NULL AS FLOAT) as latitude_rounded,
    CAST(NULL AS FLOAT) as longitude_rounded,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as created_at
FROM locations
ORDER BY location_id;
