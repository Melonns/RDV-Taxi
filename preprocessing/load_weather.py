"""Weather data loading (ELT stage): Load raw parquet → DuckDB staging.

ELT Flow:
    Extract: Raw Weather parquet files (daily/hourly)
    Load: Load ke DuckDB table weather_raw (staging)
    Transform: SQL queries untuk cleaning + feature engineering (di process_weather.py)

Handles:
- Load weather parquet files ke DuckDB
- Create weather_raw table untuk data daily
- Create weather_hourly_raw table untuk data hourly bila tersedia
"""

import logging
from pathlib import Path
from typing import List

import duckdb

logger = logging.getLogger(__name__)

def load_weather_to_duckdb(
    db_path: str,
    input_parquet_files: List[str],
) -> dict:
    """Load multiple raw weather parquet files ke DuckDB staging table.

    ELT Extract + Load stage:
    - Load raw weather parquet files
    - Create weather_raw table di DuckDB
    - Keep all data as-is (filtering akan dilakukan di SQL transform stage)

    Args:
        db_path: Path to DuckDB database file
        input_parquet_files: List of paths ke raw weather parquet files

    Returns:
        Dictionary dengan loading report
    """
    logger.info(f"Connecting to DuckDB: {db_path}")
    conn = duckdb.connect(db_path)

    try:
        logger.info(f"Loading {len(input_parquet_files)} weather parquet files to DuckDB...")

        daily_files = []
        hourly_files = []
        total_rows = 0
        hourly_row_count = 0

        for pf in input_parquet_files:
            if Path(pf).exists():
                logger.info(f"  Loading {pf}...")

                if "hourly" in Path(pf).name.lower():
                    hourly_files.append(str(pf))
                else:
                    daily_files.append(str(pf))

                df = conn.execute(f"SELECT * FROM read_parquet('{pf}')").fetch_df()
                total_rows += len(df)
                logger.info(f"    ✓ {len(df):,} rows")
            else:
                logger.warning(f"  File tidak ditemukan: {pf}")

        if total_rows == 0:
            raise ValueError("Tidak ada file weather yang berhasil di-load")

        # Create staging table weather_raw only from daily files.
        if not daily_files:
            raise ValueError("Tidak ada file weather daily yang bisa di-load ke weather_raw")

        # Load into weather_raw (Append mode)
        logger.info("\nLoading data into weather_raw (Append mode)...")
        
        # Create table if it doesn't exist
        conn.execute(f"CREATE TABLE IF NOT EXISTS weather_raw AS SELECT * FROM read_parquet('{daily_files[0]}') LIMIT 0")
        
        # Insert data (avoiding simple duplicates by checking date if table has data)
        for f in daily_files:
            conn.execute(f"INSERT INTO weather_raw SELECT * FROM read_parquet('{f}') WHERE date NOT IN (SELECT date FROM weather_raw)")

        row_count = conn.execute("SELECT COUNT(*) FROM weather_raw").fetchone()[0]
        logger.info(f"✓ weather_raw now contains {row_count:,} rows")

        # Get column info for the report
        columns = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='weather_raw'"
        ).fetchall()

        if hourly_files:
            logger.info("\nLoading data into weather_hourly_raw (Append mode)...")
            
            # Create table if it doesn't exist
            conn.execute(f"CREATE TABLE IF NOT EXISTS weather_hourly_raw AS SELECT * FROM read_parquet('{hourly_files[0]}') LIMIT 0")
            
            for f in hourly_files:
                conn.execute(f"INSERT INTO weather_hourly_raw SELECT * FROM read_parquet('{f}')")
            
            hourly_row_count = conn.execute("SELECT COUNT(*) FROM weather_hourly_raw").fetchone()[0]
            logger.info(f"✓ weather_hourly_raw now contains {hourly_row_count:,} rows")

        return {
            "db_path": str(db_path),
            "table": "weather_raw",
            "rows": row_count,
            "columns": len(columns),
            "hourly_rows": hourly_row_count,
            "status": "loaded_to_staging",
        }

    finally:
        conn.close()

if __name__ == "__main__":
    # Setup simple logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    
    # Run with default daily weather file
    load_weather_to_duckdb(
        "data/final/tlc.duckdb", 
        ["data/raw/weather/weather_daily_2025-01-01_to_2025-06-30.parquet"]
    )
