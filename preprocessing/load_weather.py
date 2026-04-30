"""Weather data loading (ELT stage): Load raw parquet → DuckDB staging.

ELT Flow:
  Extract: Raw Weather parquet files (daily/hourly)
  Load: Load ke DuckDB table weather_raw (staging)
  Transform: SQL queries untuk cleaning + feature engineering (di process_weather.py)

Handles:
- Load multiple raw weather parquet files ke DuckDB
- Create weather_raw table (staging)
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

        total_rows = 0
        loaded_files = []

        for pf in input_parquet_files:
            if Path(pf).exists():
                logger.info(f"  Loading {pf}...")

                # Read dan load ke DuckDB
                # Note: Weather data has different schema for daily/hourly, 
                # we usually process them separately but for staging we can put them in one if schema matches
                # In this project, we mostly use daily for the star schema.
                
                df = conn.execute(
                    f"SELECT * FROM read_parquet('{pf}')"
                ).fetch_df()

                loaded_files.append(str(pf))
                total_rows += len(df)
                logger.info(f"    ✓ {len(df):,} rows")
            else:
                logger.warning(f"  File tidak ditemukan: {pf}")

        if total_rows == 0:
            raise ValueError("Tidak ada file weather yang berhasil di-load")

        # Create staging table weather_raw dari semua files
        logger.info(f"\nCreating weather_raw staging table in DuckDB...")

        # Union all parquet files (assuming similar schema for the files provided)
        union_query = " UNION ALL ".join(
            [f"SELECT * FROM read_parquet('{f}')" for f in loaded_files]
        )

        conn.execute(f"DROP TABLE IF EXISTS weather_raw")
        conn.execute(f"CREATE TABLE weather_raw AS {union_query}")

        # Get row count
        row_count = conn.execute("SELECT COUNT(*) FROM weather_raw").fetchone()[0]
        logger.info(f"✓ Created weather_raw table with {row_count:,} rows")

        # Get column info
        columns = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='weather_raw'"
        ).fetchall()

        logger.info(f"✓ Table columns ({len(columns)}):")
        for col_name, col_type in columns:
            logger.info(f"    - {col_name}: {col_type}")

        return {
            "db_path": str(db_path),
            "table": "weather_raw",
            "rows": row_count,
            "columns": len(columns),
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
