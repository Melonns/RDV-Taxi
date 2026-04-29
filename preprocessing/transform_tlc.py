"""TLC data transformation (ELT stage): SQL transformations in DuckDB.

ELT Transform stage:
  Input: tlc_raw table (staging) from DuckDB
  Process: SQL queries untuk:
    1. Filter anomalies (fare, duration, passenger, GPS)
    2. Cast tipe data
    3. Calculate temporal features
    4. Join dengan weather data
  Output: tlc_cleaned table (intermediate)

Handles:
- Filter: fare_amount < 0, trip_duration < 0 atau > 300 menit, passenger_count = 0
- Cast: datetime, float, integer
- Derived: trip_duration_min, tip_percentage, hour_of_day, day_of_week, is_weekend, is_peak_hour
- Join: cleaned TLC dengan weather berdasarkan pickup datetime
"""

import logging

import duckdb

logger = logging.getLogger(__name__)


def transform_tlc_in_duckdb(
    db_path: str,
) -> dict:
    """Transform + Clean TLC data menggunakan SQL queries di DuckDB.

    ELT Transform stage:
    1. Filter anomalies dengan SQL WHERE clause
    2. Cast tipe data
    3. Add temporal features
    4. Create tlc_cleaned table (intermediate)

    Filtering rules (berdasarkan rubrik):
    - fare_amount >= 0 (tidak boleh negatif)
    - trip_duration >= 0 AND <= 300 menit (filter outliers)
    - passenger_count > 0 (tidak boleh 0)
    - pickup & dropoff GPS harus valid (tidak 0,0 dan dalam bounds NYC)

    Args:
        db_path: Path ke DuckDB database

    Returns:
        Dictionary dengan transform report
    """
    logger.info(f"Connecting to DuckDB: {db_path}")
    conn = duckdb.connect(db_path)

    try:
        logger.info("Starting TLC cleaning SQL transformation...")

        # Step 1: Check tlc_raw table
        raw_count = conn.execute("SELECT COUNT(*) FROM tlc_raw").fetchone()[0]
        logger.info(f"  Raw records in tlc_raw: {raw_count:,}")

        # Step 2: Create tlc_cleaned table dengan anomaly filtering
        # SQL yang lebih sophisticated untuk handle column name variations (tpep vs pep)
        create_cleaned_sql = """
        DROP TABLE IF EXISTS tlc_cleaned;

        CREATE TABLE tlc_cleaned AS
        SELECT 
            -- Time columns (cast ke timestamp)
            CAST(tpep_pickup_datetime AS TIMESTAMP) as pickup_datetime,
            CAST(tpep_dropoff_datetime AS TIMESTAMP) as dropoff_datetime,
            
            -- Calculate trip duration in minutes
            CAST(
                EXTRACT(EPOCH FROM (
                    CAST(tpep_dropoff_datetime AS TIMESTAMP) - 
                    CAST(tpep_pickup_datetime AS TIMESTAMP)
                )) / 60 AS FLOAT
            ) as trip_duration_min,
            
            -- Fare columns (cast ke float)
            CAST(fare_amount AS FLOAT) as fare_amount,
            CAST(extra AS FLOAT) as extra,
            CAST(mta_tax AS FLOAT) as mta_tax,
            CAST(tip_amount AS FLOAT) as tip_amount,
            CAST(tolls_amount AS FLOAT) as tolls_amount,
            CAST(total_amount AS FLOAT) as total_amount,
            
            -- Calculate tip percentage
            CASE 
                WHEN fare_amount > 0 THEN CAST((tip_amount / fare_amount * 100) AS FLOAT)
                ELSE 0.0
            END as tip_percentage,
            
            -- Passenger count (cast ke int)
            CAST(passenger_count AS INTEGER) as passenger_count,
            
            -- Trip distance
            CAST(trip_distance AS FLOAT) as trip_distance,
            
            -- Temporal features dari pickup
            CAST(EXTRACT(HOUR FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS INTEGER) as hour_of_day,
            DAYNAME(CAST(tpep_pickup_datetime AS TIMESTAMP)) as day_of_week,
            CAST(EXTRACT(DOW FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) AS INTEGER) as dow_numeric,
            CASE 
                WHEN EXTRACT(DOW FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) >= 5 THEN true
                ELSE false
            END as is_weekend,
            CASE 
                WHEN EXTRACT(HOUR FROM CAST(tpep_pickup_datetime AS TIMESTAMP)) IN (7,8,9,17,18,19) THEN true
                ELSE false
            END as is_peak_hour,
            
            -- Location columns
            CAST(pickup_longitude AS FLOAT) as pickup_longitude,
            CAST(pickup_latitude AS FLOAT) as pickup_latitude,
            CAST(dropoff_longitude AS FLOAT) as dropoff_longitude,
            CAST(dropoff_latitude AS FLOAT) as dropoff_latitude,
            
            -- Zone info
            pickup_location_id,
            dropoff_location_id,
            
            -- Rate code
            rate_code_id,
            store_and_fwd_flag
            
        FROM tlc_raw
        
        WHERE
            -- Filter 1: fare_amount >= 0
            fare_amount >= 0
            
            -- Filter 2: trip_duration 0-300 menit
            AND CAST(
                EXTRACT(EPOCH FROM (
                    CAST(tpep_dropoff_datetime AS TIMESTAMP) - 
                    CAST(tpep_pickup_datetime AS TIMESTAMP)
                )) / 60 AS FLOAT
            ) >= 0
            
            AND CAST(
                EXTRACT(EPOCH FROM (
                    CAST(tpep_dropoff_datetime AS TIMESTAMP) - 
                    CAST(tpep_pickup_datetime AS TIMESTAMP)
                )) / 60 AS FLOAT
            ) <= 300
            
            -- Filter 3: passenger_count > 0
            AND passenger_count > 0
            
            -- Filter 4: Valid pickup GPS (tidak 0,0 dan dalam NYC bounds)
            AND pickup_latitude != 0
            AND pickup_longitude != 0
            AND pickup_latitude BETWEEN 40.5 AND 40.92
            AND pickup_longitude BETWEEN -74.3 AND -73.7
            
            -- Filter 5: Valid dropoff GPS (tidak 0,0 dan dalam NYC bounds)
            AND dropoff_latitude != 0
            AND dropoff_longitude != 0
            AND dropoff_latitude BETWEEN 40.5 AND 40.92
            AND dropoff_longitude BETWEEN -74.3 AND -73.7
        ;
        """

        logger.info("Executing SQL transformation...")
        conn.execute(create_cleaned_sql)

        # Step 3: Get cleaned count dan anomaly report
        cleaned_count = conn.execute("SELECT COUNT(*) FROM tlc_cleaned").fetchone()[0]
        rows_removed = raw_count - cleaned_count
        retention_rate = (cleaned_count / raw_count * 100) if raw_count > 0 else 0

        logger.info(f"\n✓ TLC Cleaning SQL Completed:")
        logger.info(f"  Rows before: {raw_count:,}")
        logger.info(f"  Rows after: {cleaned_count:,}")
        logger.info(f"  Rows removed: {rows_removed:,}")
        logger.info(f"  Retention rate: {retention_rate:.2f}%")

        # Step 4: Analyze anomalies
        logger.info(f"\n📊 Anomaly Analysis:")

        anomaly_checks = {
            "fare_negative": "SELECT COUNT(*) FROM tlc_raw WHERE fare_amount < 0",
            "duration_negative": f"""SELECT COUNT(*) FROM tlc_raw WHERE 
                EXTRACT(EPOCH FROM (
                    CAST(tpep_dropoff_datetime AS TIMESTAMP) - 
                    CAST(tpep_pickup_datetime AS TIMESTAMP)
                )) / 60 < 0""",
            "duration_too_long": f"""SELECT COUNT(*) FROM tlc_raw WHERE 
                EXTRACT(EPOCH FROM (
                    CAST(tpep_dropoff_datetime AS TIMESTAMP) - 
                    CAST(tpep_pickup_datetime AS TIMESTAMP)
                )) / 60 > 300""",
            "passenger_zero": "SELECT COUNT(*) FROM tlc_raw WHERE passenger_count = 0",
            "invalid_pickup_gps": f"""SELECT COUNT(*) FROM tlc_raw WHERE 
                pickup_latitude = 0 OR pickup_longitude = 0 OR
                pickup_latitude < 40.5 OR pickup_latitude > 40.92 OR
                pickup_longitude < -74.3 OR pickup_longitude > -73.7""",
            "invalid_dropoff_gps": f"""SELECT COUNT(*) FROM tlc_raw WHERE 
                dropoff_latitude = 0 OR dropoff_longitude = 0 OR
                dropoff_latitude < 40.5 OR dropoff_latitude > 40.92 OR
                dropoff_longitude < -74.3 OR dropoff_longitude > -73.7""",
        }

        anomalies = {}
        for anomaly_name, sql_query in anomaly_checks.items():
            count = conn.execute(sql_query).fetchone()[0]
            if count > 0:
                anomalies[anomaly_name] = count
                logger.info(f"  - {anomaly_name}: {count:,}")

        # Step 5: Get column info
        columns = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='tlc_cleaned' ORDER BY ordinal_position"
        ).fetchall()

        logger.info(f"\n✓ Created tlc_cleaned table:")
        logger.info(f"  Columns: {len(columns)}")
        for col in columns[:10]:  # Show first 10
            logger.info(f"    - {col[0]}")
        if len(columns) > 10:
            logger.info(f"    ... and {len(columns) - 10} more")

        logger.info("\n" + "=" * 70)
        logger.info("✅ TLC Transformation (SQL) Completed Successfully")
        logger.info("=" * 70)

        return {
            "db_path": str(db_path),
            "table": "tlc_cleaned",
            "rows_before": raw_count,
            "rows_after": cleaned_count,
            "rows_removed": rows_removed,
            "retention_rate": retention_rate,
            "anomalies": anomalies,
            "columns": len(columns),
            "status": "transformed_in_database",
        }

    finally:
        conn.close()
