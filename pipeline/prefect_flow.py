"""Master Prefect pipeline orchestrator with scheduling for automated ingestion.

Koordinasi ingestion tasks:
- Open-Meteo weather data (6 months per trigger)
- NYC TLC Trip Record data (handled separately)

FITUR OTOMASI:
1. Flow bisa dijalankan manual: python pipeline/prefect_flow.py
2. Atau via Prefect deployment dengan schedule otomatis

Untuk deploy dengan scheduling:
  prefect deploy --name weather-ingestion-scheduled

Atau jalankan dengan schedule via Python:
  prefect deployment build pipeline.prefect_flow:main_pipeline \\
    --cron "0 2 * * *" \\
    --apply

ARCHITECTURE (ETL):
  Raw Data (E)
      ↓ [Ingest]
  Raw Parquet
      ↓ [Clean]
  Cleaned Parquet
      ↓ [Transform]
  Intermediate Data ← Ready untuk preprocessing & ML
"""

from datetime import datetime
from typing import Optional

from prefect import flow, get_run_logger

from ingestion.fetch_weather import ingest_weather_flow
from preprocessing.preprocessing_flow import (
    preprocessing_weather_flow,
    preprocessing_tlc_flow,
)


@flow(
    name="main_elt_pipeline",
    description="Master ELT pipeline: Weather ETL + TLC ELT into DuckDB",
)
def main_pipeline(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    weather_output_dir: Optional[str] = None,
    db_path: Optional[str] = None,
    raw_tlc_files: Optional[list] = None,
) -> dict:
    """Master pipeline orchestrator untuk weather + TLC ELT processing.

    ARCHITECTURE:
      Weather ETL (Python transform → intermediate parquet):
        Raw Parquet → Clean → Transform → weather_intermediate/

      TLC ELT (SQL transform in DuckDB):
        Raw Parquet → DuckDB(tlc_raw) → SQL Transform → DuckDB(tlc_cleaned)

    Both intermediate stages stored di DuckDB untuk next modeling stage.

    Args:
        start_date: Weather start date (YYYY-MM-DD). Default: 2025-01-01
        end_date: Weather end date (YYYY-MM-DD). Default: 2025-06-30
        weather_output_dir: Output directory untuk weather intermediate files
        db_path: DuckDB database path. Default: data/final/tlc.duckdb
        raw_tlc_files: List of raw TLC parquet file paths (optional)

    Returns:
        Dictionary dengan hasil ingestion + preprocessing weather + TLC
    """
    logger = get_run_logger()

    # Set defaults
    if db_path is None:
        db_path = "./data/final/tlc.duckdb"

    logger.info("=" * 70)
    logger.info("🚀 Starting Main ELT Pipeline - Weather + TLC Processing")
    logger.info("=" * 70)
    logger.info(f"⏰ Timestamp: {datetime.now().isoformat()}")
    logger.info(f"📊 DuckDB: {db_path}")

    results = {}

    # ===== STAGE 1: EXTRACT (Ingestion) =====
    logger.info("\n" + "-" * 70)
    logger.info("[STAGE 1] EXTRACT - Ingesting weather data from Open-Meteo API")
    logger.info("-" * 70)

    try:
        weather_result = ingest_weather_flow(
            start_date=start_date,
            end_date=end_date,
            output_dir=weather_output_dir,
        )
        results["weather_ingestion"] = weather_result

        logger.info(f"✓ Weather ingestion completed successfully!")
        logger.info(f"  📊 Date range: {weather_result['date_range']}")
        logger.info(f"  ⏱️  Hourly records: {weather_result['records_hourly']}")
        logger.info(f"  📈 Daily aggregates: {weather_result['records_daily']}")

    except Exception as e:
        logger.error(f"✗ Weather ingestion failed: {str(e)}")
        raise

    # ===== STAGE 2A: TRANSFORM Weather (ETL) =====
    logger.info("\n" + "-" * 70)
    logger.info("[STAGE 2A] TRANSFORM - Weather cleaning + feature engineering (ETL)")
    logger.info("-" * 70)

    try:
        preprocessing_result = preprocessing_weather_flow(
            raw_hourly_file=weather_result["hourly_file"],
            raw_daily_file=weather_result["daily_file"],
            output_dir=weather_output_dir,
        )
        results["weather_preprocessing"] = preprocessing_result

        logger.info(f"✓ Weather preprocessing completed successfully!")
        logger.info(f"  Hourly transformed: {preprocessing_result['hourly_transformed']['rows']} rows")
        logger.info(f"  Daily transformed: {preprocessing_result['daily_transformed']['rows']} rows")

    except Exception as e:
        logger.error(f"✗ Weather preprocessing failed: {str(e)}")
        raise

    # ===== STAGE 2B: TRANSFORM TLC (ELT in DuckDB) =====
    logger.info("\n" + "-" * 70)
    logger.info("[STAGE 2B] TRANSFORM - TLC ELT (Load raw → Transform in DuckDB SQL)")
    logger.info("-" * 70)

    if raw_tlc_files:
        try:
            tlc_result = preprocessing_tlc_flow(
                raw_tlc_files=raw_tlc_files,
                db_path=db_path,
            )
            results["tlc_preprocessing"] = tlc_result

            logger.info(f"✓ TLC ELT completed successfully!")
            logger.info(f"  Staging table: tlc_raw ({tlc_result['load']['rows']:,} rows)")
            logger.info(f"  Intermediate table: tlc_cleaned ({tlc_result['transform']['rows_after']:,} rows)")
            logger.info(f"  Retention rate: {tlc_result['transform']['retention_rate']:.2f}%")

        except Exception as e:
            logger.error(f"✗ TLC ELT failed: {str(e)}")
            raise
    else:
        logger.warning("⚠️  No TLC files provided - skipping TLC ELT stage")
        results["tlc_preprocessing"] = {"status": "skipped"}

    # ===== STAGE 3: Summary =====
    logger.info("\n" + "-" * 70)
    logger.info("[STAGE 3] SUMMARY - Intermediate data ready for modeling")
    logger.info("-" * 70)

    logger.info(f"✓ Weather intermediate data (parquet):")
    logger.info(f"  📁 {preprocessing_result['hourly_transformed']['output_file']}")
    logger.info(f"  📁 {preprocessing_result['daily_transformed']['output_file']}")

    if raw_tlc_files:
        logger.info(f"✓ TLC intermediate data (DuckDB):")
        logger.info(f"  📊 Database: {db_path}")
        logger.info(f"  📋 Tables: tlc_raw, tlc_cleaned")

    logger.info("\n" + "=" * 70)
    logger.info("✅ Main ELT Pipeline Completed Successfully")
    logger.info("=" * 70)

    return results


# ===================================================================
# OTOMASI / SCHEDULING
# ===================================================================
# Ada 3 cara untuk menjalankan flow dengan schedule otomatis:
#
# 1. MANUAL (Sekarang):
#    python pipeline/prefect_flow.py
#
# 2. VIA PREFECT CLI (Recommended):
#    prefect deployment build pipeline/prefect_flow.py:main_pipeline \
#      --name "weather-etl" \
#      --cron "0 2 * * *" \
#      --apply
#    # Ini akan jalankan flow otomatis setiap hari jam 2 AM
#
# 3. VIA PREFECT WORKER:
#    prefect worker start
#    # Di terminal lain:
#    prefect deployment build pipeline/prefect_flow.py:main_pipeline --apply


if __name__ == "__main__":
    result = main_pipeline()

    print("\n" + "=" * 70)
    print("📊 PIPELINE EXECUTION SUMMARY (ELT)")
    print("=" * 70)

    weather_ingestion = result.get("weather_ingestion", {})
    weather_preprocessing = result.get("weather_preprocessing", {})
    tlc_preprocessing = result.get("tlc_preprocessing", {})

    print(f"\n✅ [EXTRACT] Weather Ingestion:")
    print(f"   📅 Date range: {weather_ingestion.get('date_range', 'N/A')}")
    print(f"   ⏱️  Hourly records: {weather_ingestion.get('records_hourly', 0):,}")
    print(f"   📈 Daily records: {weather_ingestion.get('records_daily', 0):,}")

    print(f"\n✅ [TRANSFORM] Weather Preprocessing (ETL):")

    if weather_preprocessing.get("hourly_transformed"):
        hourly = weather_preprocessing["hourly_transformed"]
        print(f"   Hourly cleaned + transformed:")
        print(f"     - Rows: {hourly.get('rows', 0):,}")
        print(f"     - Output: {hourly.get('output_file', 'N/A')}")

    if weather_preprocessing.get("daily_transformed"):
        daily = weather_preprocessing["daily_transformed"]
        print(f"   Daily cleaned + transformed:")
        print(f"     - Rows: {daily.get('rows', 0):,}")
        print(f"     - Output: {daily.get('output_file', 'N/A')}")

    if tlc_preprocessing.get("status") != "skipped":
        print(f"\n✅ [TRANSFORM] TLC ELT (Load → Transform in DuckDB):")
        
        if tlc_preprocessing.get("load"):
            load_data = tlc_preprocessing["load"]
            print(f"   Stage 1 - Load to staging:")
            print(f"     - Table: {load_data.get('table', 'N/A')}")
            print(f"     - Rows: {load_data.get('rows', 0):,}")

        if tlc_preprocessing.get("transform"):
            transform_data = tlc_preprocessing["transform"]
            print(f"   Stage 2 - Transform in DB:")
            print(f"     - Rows before: {transform_data.get('rows_before', 0):,}")
            print(f"     - Rows after: {transform_data.get('rows_after', 0):,}")
            print(f"     - Retention: {transform_data.get('retention_rate', 0):.2f}%")
            if transform_data.get("anomalies"):
                print(f"     - Anomalies filtered: {len(transform_data['anomalies'])} types")

    print("\n" + "=" * 70)
    print("🎯 Next Steps:")
    print("  1. Create dbt/SQL models for star schema (fact_trips + dimensions)")
    print("  2. Load final data to DuckDB (data/final/tlc.duckdb)")
    print("  3. SQL analysis + ML modeling")
    print("  4. Dashboard visualization")
    print("=" * 70)
