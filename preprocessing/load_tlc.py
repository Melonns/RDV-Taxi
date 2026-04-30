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
        skipped_files = []

        table_exists = False
        metadata_exists = False
        try:
            table_exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'tlc_raw'"
            ).fetchone()[0] > 0
            metadata_exists = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'tlc_loaded_files'"
            ).fetchone()[0] > 0
        except Exception:
            table_exists = False
            metadata_exists = False

        legacy_database = table_exists and not metadata_exists

        if legacy_database:
            raise RuntimeError(
                "Legacy DuckDB detected: tlc_raw already exists without tlc_loaded_files metadata. "
                "Delete data/final/tlc.duckdb once to start in cumulative batch mode from a clean slate."
            )

        if not metadata_exists:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tlc_loaded_files (
                    file_path VARCHAR PRIMARY KEY,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            metadata_exists = True

        for pf in input_parquet_files:
            if Path(pf).exists():
                already_loaded = conn.execute(
                    "SELECT COUNT(*) FROM tlc_loaded_files WHERE file_path = ?",
                    [str(pf)],
                ).fetchone()[0] > 0

                if already_loaded:
                    logger.info(f"  Skipping already-loaded file: {pf}")
                    skipped_files.append(str(pf))
                    continue

                logger.info(f"  Loading {pf}...")

                # Count rows without materializing the entire parquet into memory
                df_rows = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{pf}')"
                ).fetchone()[0]

                loaded_files.append(str(pf))
                total_rows += int(df_rows)
                logger.info(f"    ✓ {df_rows:,} rows")
            else:
                logger.warning(f"  File tidak ditemukan: {pf}")

        if total_rows == 0 and not table_exists:
            raise ValueError("Tidak ada file TLC yang berhasil di-load")

        # Create or append staging table tlc_raw
        if loaded_files:
            logger.info(f"\nLoading into tlc_raw staging table in DuckDB...")

            for file_path in loaded_files:
                if not table_exists:
                    conn.execute(
                        f"CREATE TABLE tlc_raw AS SELECT * FROM read_parquet('{file_path}')"
                    )
                    table_exists = True
                else:
                    conn.execute(f"INSERT INTO tlc_raw SELECT * FROM read_parquet('{file_path}')")

                conn.execute(
                    "INSERT OR REPLACE INTO tlc_loaded_files (file_path) VALUES (?)",
                    [str(file_path)],
                )

        # Get row count (if table exists)
        row_count = conn.execute("SELECT COUNT(*) FROM tlc_raw").fetchone()[0] if table_exists else 0
        logger.info(f"✓ tlc_raw now contains {row_count:,} rows")

        # Get column info
        columns = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name='tlc_raw'"
        ).fetchall()

        logger.info(f"✓ Table columns ({len(columns)}):")
        for col_name, col_type in columns:
            logger.info(f"    - {col_name}: {col_type}")

        logger.info("\n📊 TLC Loading Summary:")
        logger.info(f"  Files loaded: {len(loaded_files)}")
        logger.info(f"  Files skipped (already loaded): {len(skipped_files)}")
        logger.info(f"  Total rows: {row_count:,}")
        logger.info(f"  Database: {db_path}")
        logger.info(f"  Table: tlc_raw")
        logger.info(f"  Stage: Staging (raw, unfiltered, cumulative)")

        return {
            "db_path": str(db_path),
            "table": "tlc_raw",
            "rows": row_count,
            "columns": len(columns),
            "metadata_table": "tlc_loaded_files",
            "loaded_files": loaded_files,
            "skipped_files": skipped_files,
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
