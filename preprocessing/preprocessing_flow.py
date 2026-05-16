"""
Kelompok Proyek Rekayasa Data:
1. 235150201111036 DARVESH AZIZ MAWLA
2. 235150207111063 ACHMAD ALVIAN PRASETIO
3. 235150207111006 DZAKY REZANDI
4. 235150201111004 WAHYU DWI LAKSANA PUTRI
5. 235150207111065 JONATHAN SALIM
"""

"""Preprocessing flow: Clean dan Transform weather + TLC data dengan Prefect."""

import os
from pathlib import Path
from typing import Optional

from prefect import flow, get_run_logger, task

from preprocessing.load_tlc import load_tlc_to_duckdb
from preprocessing.process_tlc import transform_tlc_in_duckdb
from preprocessing.load_weather import load_weather_to_duckdb
from preprocessing.process_weather import transform_weather_in_duckdb


@task
def load_weather_to_db_task(
    input_files: list,
    db_path: str,
) -> dict:
    """Prefect task untuk ELT Extract+Load: Weather parquet → DuckDB staging."""
    logger = get_run_logger()

    logger.info(f"[ELT LOAD] Weather files: {len(input_files)}")
    logger.info(f"[ELT LOAD] Database: {db_path}")

    result = load_weather_to_duckdb(db_path, input_files)

    logger.info(f"✓ Weather loaded to DuckDB staging")
    return result


@task
def transform_weather_in_db_task(
    db_path: str,
) -> dict:
    """Prefect task untuk ELT Transform: SQL cleaning+features in DuckDB."""
    logger = get_run_logger()

    logger.info(f"[ELT TRANSFORM] Weather Database: {db_path}")

    result = transform_weather_in_duckdb(db_path)

    logger.info(f"✓ Weather transformed in DuckDB")
    logger.info(f"  Rows: {result['rows_after']:,}")

    return result


@task
def load_tlc_to_db_task(
    input_files: list,
    db_path: str,
) -> dict:
    """Prefect task untuk ELT Extract+Load: TLC parquet → DuckDB staging."""
    logger = get_run_logger()

    logger.info(f"[ELT LOAD] Input files: {len(input_files)} file(s)")
    logger.info(f"[ELT LOAD] Database: {db_path}")

    result = load_tlc_to_duckdb(db_path, input_files)

    logger.info(f"✓ TLC loaded to DuckDB staging")
    logger.info(f"  Table: {result['table']}")
    logger.info(f"  Rows: {result['rows']:,}")

    return result


@task
def transform_tlc_in_db_task(
    db_path: str,
) -> dict:
    """Prefect task untuk ELT Transform: SQL cleaning+features in DuckDB."""
    logger = get_run_logger()

    logger.info(f"[ELT TRANSFORM] Database: {db_path}")

    result = transform_tlc_in_duckdb(db_path)

    logger.info(f"✓ TLC transformed in DuckDB")
    logger.info(f"  Before: {result['rows_before']:,}")
    logger.info(f"  After: {result['rows_after']:,}")
    logger.info(f"  Retention: {result['retention_rate']:.2f}%")

    return result

@flow(name="preprocessing_weather_flow", description="SQL-based Weather ELT")
def preprocessing_weather_flow(
    raw_weather_files: list,
    db_path: str,
) -> dict:
    """Preprocessing flow untuk weather data menggunakan ELT.

    Args:
        raw_weather_files: List of raw weather parquet file paths
        db_path: Path to DuckDB database

    Returns:
        Dictionary dengan ELT results
    """
    logger = get_run_logger()

    logger.info("=" * 70)
    logger.info("🌤️  Starting Weather ELT Pipeline (Extract-Load-Transform)")
    logger.info("=" * 70)

    results = {}

    # LOAD
    logger.info("\n[STAGE 1] Loading raw weather to staging...")
    load_result = load_weather_to_db_task(raw_weather_files, db_path)
    results["load"] = load_result

    # TRANSFORM
    logger.info("\n[STAGE 2] Transforming weather in DuckDB...")
    transform_result = transform_weather_in_db_task(db_path)
    results["transform"] = transform_result

    return results


@flow(name="preprocessing_tlc_flow", description="ELT for TLC: Load raw → DuckDB → Transform with SQL")
def preprocessing_tlc_flow(
    raw_tlc_files: list,
    db_path: str,
) -> dict:
    """Preprocessing flow untuk TLC data: ELT (Extract-Load-Transform).

    Flow stages:
      1. Extract: Raw TLC parquet files
      2. Load: Load ke DuckDB table tlc_raw (staging)
      3. Transform: SQL queries untuk cleaning + features dalam DuckDB

    Args:
        raw_tlc_files: List of raw TLC parquet file paths
        db_path: Path to DuckDB database file (data/final/tlc.duckdb)

    Returns:
        Dictionary dengan ELT results dan anomaly report
    """
    logger = get_run_logger()

    logger.info("=" * 70)
    logger.info("🔄 Starting TLC ELT Pipeline (Extract-Load-Transform)")
    logger.info("=" * 70)

    results = {}

    # EXTRACT + LOAD: Raw parquet → DuckDB staging
    logger.info("\n[STAGE 1] EXTRACT-LOAD: Loading raw TLC to DuckDB staging...")
    try:
        load_result = load_tlc_to_db_task(raw_tlc_files, db_path)
        results["load"] = load_result
        logger.info(f"✓ TLC loaded to staging (tlc_raw table)")
    except Exception as e:
        logger.error(f"✗ TLC load failed: {str(e)}")
        raise

    # TRANSFORM: SQL cleaning + features in DuckDB
    logger.info("\n[STAGE 2] TRANSFORM: SQL cleaning + features in DuckDB...")
    try:
        transform_result = transform_tlc_in_db_task(db_path)
        results["transform"] = transform_result
        logger.info(f"✓ TLC transformed (tlc_cleaned table)")

        if transform_result.get("anomalies"):
            logger.info(f"\n  Anomalies detected and filtered:")
            for anomaly, count in transform_result["anomalies"].items():
                logger.info(f"    - {anomaly}: {count:,}")

    except Exception as e:
        logger.error(f"✗ TLC transform failed: {str(e)}")
        raise

    logger.info("\n" + "=" * 70)
    logger.info("✅ TLC ELT Pipeline Completed Successfully")
    logger.info(f"  Database: {db_path}")
    logger.info(f"  Tables created: tlc_raw (staging), tlc_cleaned (intermediate)")
    
    return results



