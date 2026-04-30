"""Weather data transformation (ELT stage): SQL transformations in DuckDB.

ELT Transform stage:
  Input: weather_raw table (staging) from DuckDB
  Process: SQL queries untuk:
    1. Filter anomalies (temperature, precipitation ranges)
    2. Handle missing values (Forward Fill menggunakan LAST_VALUE)
    3. Calculate temporal features (hour, day_of_week, is_weekend)
    4. Categorize temperature
  Output: weather_transformed table (intermediate)
"""

import logging
import duckdb

logger = logging.getLogger(__name__)

def transform_weather_in_duckdb(db_path: str) -> dict:
    """Transform + Clean Weather data menggunakan SQL queries di DuckDB.

    ELT Transform stage:
    1. Handle missing values with Forward Fill (ffill)
    2. Filter anomalies & Validate ranges
    3. Add temporal features
    4. Create weather_transformed table
    """
    logger.info(f"Connecting to DuckDB: {db_path}")
    conn = duckdb.connect(db_path)

    try:
        logger.info("Starting Weather cleaning SQL transformation...")

        # Step 1: Check weather_raw table
        raw_count = conn.execute("SELECT COUNT(*) FROM weather_raw").fetchone()[0]
        logger.info(f"  Raw records in weather_raw: {raw_count:,}")

        # Step 2: Create weather_transformed table
        # Kita pakai window function LAST_VALUE(val IGNORE NULLS) untuk ffill di SQL
        
        # Cek apakah kolom-kolom yang dibutuhkan ada
        cols = [c[0] for c in conn.execute("DESCRIBE weather_raw").fetchall()]
        
        # Tentukan kolom mana yang mau di-ffill (biasanya kolom numerik cuaca)
        weather_cols = [
            "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
            "precipitation_sum", "relative_humidity_2m_mean", "wind_speed_10m_max"
        ]
        available_cols = [c for c in weather_cols if c in cols]
        
        # Build ffill queries
        ffill_selects = []
        for col in available_cols:
            ffill_selects.append(f"LAST_VALUE({col} IGNORE NULLS) OVER (ORDER BY date) as {col}")
        
        ffill_query = ", ".join(ffill_selects)
        
        create_sql = f"""
        DROP TABLE IF EXISTS weather_transformed;

        CREATE TABLE weather_transformed AS
        WITH ffilled AS (
            SELECT 
                date,
                {ffill_query if available_cols else "*"}
            FROM weather_raw
        )
        SELECT 
            CAST(date AS DATE) as date_actual,
            
            -- Validation & Clipping
            CASE 
                WHEN temperature_2m_mean < -40 THEN -40 
                WHEN temperature_2m_mean > 50 THEN 50 
                ELSE temperature_2m_mean 
            END as avg_temperature,
            
            temperature_2m_min as min_temperature,
            temperature_2m_max as max_temperature,
            
            CASE 
                WHEN precipitation_sum < 0 THEN 0 
                WHEN precipitation_sum > 500 THEN 500 
                ELSE precipitation_sum 
            END as total_precipitation,
            
            relative_humidity_2m_mean as avg_humidity,
            
            -- Temporal features
            EXTRACT(YEAR FROM CAST(date AS DATE)) as year,
            EXTRACT(MONTH FROM CAST(date AS DATE)) as month,
            DAYNAME(CAST(date AS DATE)) as day_of_week,
            CASE WHEN EXTRACT(DOW FROM CAST(date AS DATE)) >= 5 THEN true ELSE false END as is_weekend,
            
            -- Temperature Category
            CASE
                WHEN temperature_2m_mean < 0 THEN 'Cold'
                WHEN temperature_2m_mean BETWEEN 0 AND 15 THEN 'Cool'
                WHEN temperature_2m_mean BETWEEN 15 AND 25 THEN 'Mild'
                WHEN temperature_2m_mean BETWEEN 25 AND 35 THEN 'Warm'
                ELSE 'Hot'
            END as temperature_category

        FROM ffilled
        WHERE date IS NOT NULL;
        """

        logger.info("Executing SQL transformation...")
        conn.execute(create_sql)

        # Step 3: Get report
        transformed_count = conn.execute("SELECT COUNT(*) FROM weather_transformed").fetchone()[0]
        
        logger.info(f"\n✓ Weather Cleaning SQL Completed:")
        logger.info(f"  Rows before: {raw_count:,}")
        logger.info(f"  Rows after: {transformed_count:,}")

        return {
            "db_path": str(db_path),
            "table": "weather_transformed",
            "rows_before": raw_count,
            "rows_after": transformed_count,
            "status": "transformed_in_database",
        }

    finally:
        conn.close()

if __name__ == "__main__":
    # Setup simple logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    
    transform_weather_in_duckdb("data/final/tlc.duckdb")
