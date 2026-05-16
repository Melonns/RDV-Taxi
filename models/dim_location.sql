/*
Kelompok Proyek Rekayasa Data:
1. 235150201111036 DARVESH AZIZ MAWLA
2. 235150207111063 ACHMAD ALVIAN PRASETIO
3. 235150207111006 DZAKY REZANDI
4. 235150201111004 WAHYU DWI LAKSANA PUTRI
5. 235150207111065 JONATHAN SALIM
*/

-- DIM_LOCATION: Dimension table untuk pickup/dropoff locations
-- Generated from unique PULocationID/DOLocationIDs
-- Centroid lat/lon di-JOIN dari taxi_zone_lookup (hasil ingest_zone_lookup_flow)

DROP TABLE IF EXISTS dim_location;

CREATE TABLE dim_location AS
WITH locations AS (
    -- Kumpulkan semua unique location IDs dari pickup & dropoff
    SELECT DISTINCT pickup_location_id AS location_id
    FROM tlc_cleaned
    WHERE pickup_location_id IS NOT NULL

    UNION

    SELECT DISTINCT dropoff_location_id AS location_id
    FROM tlc_cleaned
    WHERE dropoff_location_id IS NOT NULL
)
SELECT
    ROW_NUMBER() OVER (ORDER BY l.location_id)   AS location_key,
    l.location_id,

    -- Koordinat centroid dari polygon zona (NULL jika zone lookup belum di-ingest)
    tzl.latitude                                  AS latitude,
    tzl.longitude                                 AS longitude,

    -- Koordinat dibulatkan 4 desimal untuk grouping/binning
    ROUND(tzl.latitude, 4)                        AS latitude_rounded,
    ROUND(tzl.longitude, 4)                       AS longitude_rounded,

    -- Metadata zona dari TLC official lookup
    tzl.zone                                      AS zone_name,
    tzl.borough                                   AS borough,
    tzl.service_zone                              AS service_zone,

    CAST(CURRENT_TIMESTAMP AS TIMESTAMP)          AS created_at

FROM locations l
LEFT JOIN taxi_zone_lookup tzl
       ON l.location_id = tzl.location_id

ORDER BY l.location_id;



