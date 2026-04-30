"""TLC data cleaning (ELT stage): Load raw parquet → DuckDB staging.

ELT Flow:
  Extract: Raw TLC parquet files
  Load: Load ke DuckDB table tlc_raw (staging)
  Transform: SQL queries untuk cleaning + filtering (di transform_tlc.py)

Handles:
- Load multiple raw TLC parquet files ke DuckDB
- Create tlc_raw table (staging)
- Log anomalies yang akan di-filter di SQL stage
"""

import logging
from pathlib import Path
from typing import List

import duckdb

logger = logging.getLogger(__name__)

def load_tlc_to_duckdb(
    db_path: str,
    input_parquet_files: List[str],
) -> dict:
    """Load multiple raw TLC parquet files ke DuckDB staging table.

    ELT Extract + Load stage:
    - Load raw TLC parquet files
    - Create tlc_raw table di DuckDB
    - Keep all data as-is (no filtering yet - akan di-filter di SQL transform stage)

    Args:
        db_path: Path to DuckDB database file (akan dibuat jika belum ada)
        input_parquet_files: List of paths ke raw TLC parquet files

    Returns:
        Dictionary dengan loading report
    """
    logger.info(f"Connecting to DuckDB: {db_path}")
    conn = duckdb.connect(db_path)

    try:
        logger.info(f"Loading {len(input_parquet_files)} TLC parquet files to DuckDB...")

        total_rows = 0
        loaded_files = []

        for pf in input_parquet_files:
            if Path(pf).exists():
                logger.info(f"  Loading {pf}...")

                # Read dan load ke DuckDB
                df = conn.execute(
                    f"SELECT * FROM read_parquet('{pf}')"
                ).fetch_df()

                loaded_files.append(str(pf))
                total_rows += len(df)
                logger.info(f"    ✓ {len(df):,} rows")
            else:
                logger.warning(f"  File tidak ditemukan: {pf}")

        if total_rows == 0:
            raise ValueError("Tidak ada file TLC yang berhasil di-load")

        # Create staging table tlc_raw dari semua files
        logger.info(f"\nCreating tlc_raw staging table in DuckDB...")

        # Union all parquet files
        union_query = " UNION ALL ".join(
            [f"SELECT * FROM read_parquet('{f}')" for f in loaded_files]
        )

        conn.execute(f"DROP TABLE IF EXISTS tlc_raw")
        conn.execute(f"CREATE TABLE tlc_raw AS {union_query}")

        # Get row count
        row_count = conn.execute("SELECT COUNT(*) FROM tlc_raw").fetchone()[0]
        logger.info(f"✓ Created tlc_raw table with {row_count:,} rows")

        # Get column info
        columns = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='tlc_raw'"
        ).fetchall()

        logger.info(f"✓ Table columns ({len(columns)}):")
        for col_name, col_type in columns:
            logger.info(f"    - {col_name}: {col_type}")

        logger.info("\n📊 TLC Loading Summary:")
        logger.info(f"  Files loaded: {len(loaded_files)}")
        logger.info(f"  Total rows: {row_count:,}")
        logger.info(f"  Database: {db_path}")
        logger.info(f"  Table: tlc_raw")
        logger.info(f"  Stage: Staging (raw, unfiltered)")

        return {
            "db_path": str(db_path),
            "table": "tlc_raw",
            "rows": row_count,
            "columns": len(columns),
            "status": "loaded_to_staging",
        }

    finally:
        conn.close()

if __name__ == "__main__":
    # Setup simple logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    
    # Run for the first month as default test
    load_tlc_to_duckdb(
        "data/final/tlc.duckdb", 
        ["data/raw/tlc/yellow_tripdata_2025-01.parquet"]
    )
