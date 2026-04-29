"""Preprocessing flow: Clean dan Transform weather + TLC data dengan Prefect."""

import os
from pathlib import Path
from typing import Optional

from prefect import flow, get_run_logger, task

from preprocessing.clean import clean_raw_data
from preprocessing.transform import transform_data
from preprocessing.clean_tlc import load_tlc_to_duckdb
from preprocessing.transform_tlc import transform_tlc_in_duckdb


@task
def clean_weather_task(
    input_file: str,
    output_dir: Optional[str] = None,
) -> dict:
    """Prefect task untuk cleaning weather data."""
    logger = get_run_logger()

    if output_dir is None:
        output_dir = os.path.join(
            os.getenv("INTERMEDIATE_DATA_PATH", "./data/intermediate"),
            "weather",
        )

    input_path = Path(input_file)
    output_file = os.path.join(output_dir, f"weather_cleaned_{input_path.stem}.parquet")

    logger.info(f"[CLEANING] Input: {input_file}")
    logger.info(f"[CLEANING] Output: {output_file}")

    result = clean_raw_data(input_file, output_file)

    logger.info(f"✓ Cleaning completed")
    logger.info(f"  Rows before: {result['report']['total_rows_before']}")
    logger.info(f"  Rows after: {result['report']['total_rows_after']}")
    logger.info(f"  Rows removed: {result['report']['rows_removed']}")

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



    input_file: str,
    output_dir: Optional[str] = None,
) -> dict:
    """Prefect task untuk transform weather data."""
    logger = get_run_logger()

    if output_dir is None:
        output_dir = os.path.join(
            os.getenv("INTERMEDIATE_DATA_PATH", "./data/intermediate"),
            "weather",
        )

    input_path = Path(input_file)
    output_file = os.path.join(output_dir, f"weather_transformed_{input_path.stem}.parquet")

    logger.info(f"[TRANSFORM] Input: {input_file}")
    logger.info(f"[TRANSFORM] Output: {output_file}")

    result = transform_data(input_file, output_file, add_features=True)

    logger.info(f"✓ Transform completed")
    logger.info(f"  Rows: {result['rows']}")
    logger.info(f"  Columns: {len(result['columns'])}")

    return result


@flow(name="preprocessing_weather_flow", description="Clean + Transform weather data")
def preprocessing_weather_flow(
    raw_hourly_file: str,
    raw_daily_file: str,
    output_dir: Optional[str] = None,
) -> dict:
    """Preprocessing flow untuk weather data.

    Args:
        raw_hourly_file: Path to raw hourly weather parquet
        raw_daily_file: Path to raw daily weather parquet
        output_dir: Output directory untuk intermediate data

    Returns:
        Dictionary dengan cleaned + transformed file paths
    """
    logger = get_run_logger()

    logger.info("=" * 70)
    logger.info("🧹 Starting Weather Preprocessing (Clean + Transform)")
    logger.info("=" * 70)

    results = {}

    # Clean hourly data
    logger.info("\n[STAGE 1A] Cleaning hourly weather data...")
    try:
        hourly_clean_result = clean_weather_task(raw_hourly_file, output_dir)
        results["hourly_clean"] = hourly_clean_result
        logger.info(f"✓ Hourly cleaning done")
    except Exception as e:
        logger.error(f"✗ Hourly cleaning failed: {str(e)}")
        raise

    # Transform hourly data
    logger.info("\n[STAGE 1B] Transforming hourly weather data...")
    try:
        hourly_transform_result = transform_weather_task(
            hourly_clean_result["output_file"],
            output_dir,
        )
        results["hourly_transformed"] = hourly_transform_result
        logger.info(f"✓ Hourly transform done")
    except Exception as e:
        logger.error(f"✗ Hourly transform failed: {str(e)}")
        raise

    # Clean daily data
    logger.info("\n[STAGE 2A] Cleaning daily weather data...")
    try:
        daily_clean_result = clean_weather_task(raw_daily_file, output_dir)
        results["daily_clean"] = daily_clean_result
        logger.info(f"✓ Daily cleaning done")
    except Exception as e:
        logger.error(f"✗ Daily cleaning failed: {str(e)}")
        raise

    # Transform daily data
    logger.info("\n[STAGE 2B] Transforming daily weather data...")
    try:
        daily_transform_result = transform_weather_task(
            daily_clean_result["output_file"],
            output_dir,
        )
        results["daily_transformed"] = daily_transform_result
        logger.info(f"✓ Daily transform done")
    except Exception as e:
        logger.error(f"✗ Daily transform failed: {str(e)}")
        raise

    logger.info("\n" + "=" * 70)
    logger.info("✅ Weather Preprocessing Completed Successfully")
    logger.info("=" * 70)

    return results


@flow(name="preprocessing_tlc_flow", description="Clean + Transform + Join TLC with weather")
def preprocessing_tlc_flow(
    raw_tlc_files: list,
    weather_transformed_file: str,
    output_dir: Optional[str] = None,
) -> dict:
    """Preprocessing flow untuk TLC data: clean + transform + join weather.

    Args:
        raw_tlc_files: List of raw TLC parquet file paths
        weather_transformed_file: Path to transformed weather parquet
        output_dir: Output directory untuk intermediate data

    Returns:
        Dictionary dengan cleaned + transformed + joined file paths
    """
    logger = get_run_logger()

    logger.info("=" * 70)
    logger.info("🧹 Starting TLC Preprocessing (Clean + Transform + Join Weather)")
    logger.info("=" * 70)ELT for TLC: Load raw → DuckDB → Transform with SQL")
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

        if transform_result["anomalies"]:
            logger.info(f"\n  Anomalies detected and filtered:")
            for anomaly, count in transform_result["anomalies"].items():
                logger.info(f"    - {anomaly}: {count:,}")

    except Exception as e:
        logger.error(f"✗ TLC transform failed: {str(e)}")
        raise

    logger.info("\n" + "=" * 70)
    logger.info("✅ TLC ELT Pipeline Completed Successfully")
    logger.info(f"  Database: {db_path}")
    logger.info(f"  Tables created: tlc_raw (staging), tlc_cleaned (intermediate)
