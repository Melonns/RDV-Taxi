"""Master Prefect pipeline orchestrator with scheduling for automated ingestion.

Koordinasi ingestion tasks:
- Open-Meteo weather data (6 months per trigger)
- NYC TLC Trip Record data (handled separately)
- Zona Taxi

FITUR OTOMASI:
1. Flow bisa dijalankan manual: python pipeline/prefect_flow.py
2. Atau via Prefect deployment dengan schedule otomatis

Untuk deploy dengan scheduling:
  prefect deploy --name weather-ingestion-scheduled

Atau jalankan dengan schedule via Python:
  prefect deployment build pipeline.prefect_flow:main_pipeline \\
        --cron "0 0 1 * *" \
    --apply


"""

import sys
import os

# Add project root to Python path untuk import modules
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from datetime import datetime
from typing import Optional

from prefect import flow, get_run_logger

from ingestion.fetch_weather import ingest_weather_flow
from ingestion.ingest_nyc import ingest_nyc_flow
from ingestion.ingest_zone import ingest_zone_lookup_flow
# ELT helpers (Load -> Transform)
from preprocessing.load_tlc import load_tlc_to_duckdb
from preprocessing.process_tlc import transform_tlc_in_duckdb
from preprocessing.load_weather import load_weather_to_duckdb
from preprocessing.process_weather import transform_weather_in_duckdb
# Star schema creation (final stage)
from pipeline.load_star_schema import create_star_schema, generate_schema_summary


@flow(
    name="main_elt_pipeline",
    description="Master ELT pipeline: Zone + NYC Taxi + Weather ingestion dengan otomasi",
)
def main_pipeline(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    weather_output_dir: Optional[str] = None,
    db_path: Optional[str] = None,
    raw_tlc_files: Optional[list] = None,
    skip_zone_ingestion: bool = False,
    skip_weather_ingestion: bool = False,
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
        skip_zone_ingestion: Skip zone lookup ingestion (use existing data). Default: False
        skip_weather_ingestion: Skip weather ingestion (use existing data). Default: False

    Returns:
        Dictionary dengan hasil ingestion
    """
    logger = get_run_logger()

    # Set defaults
    if db_path is None:
        db_path = "./data/final/tlc.duckdb"

    logger.info("=" * 70)
    logger.info("🚀 Starting Main ELT  Pipeline - Zone + NYC Taxi + Weather Ingestion")
    logger.info("=" * 70)
    logger.info(f"⏰ Timestamp: {datetime.now().isoformat()}")
    logger.info(f"📊 DuckDB: {db_path}")

    results = {}

    # ===== STAGE 1A: EXTRACT (Taxi Zone Lookup) =====
    # ⚠️  Zone data jarang berubah - jalankan 1x saja, skip untuk frequent runs
    if not skip_zone_ingestion:
        logger.info("\n" + "-" * 70)
        logger.info("[STAGE 1A] EXTRACT - Ingesting Taxi Zone Lookup data")
        logger.info("-" * 70)

        try:
            ingest_zone_lookup_flow()
            results["zone"] = {"status": "success"}
            logger.info("✓ Taxi Zone ingestion completed successfully!")

        except Exception as e:
            logger.error(f"✗ Taxi Zone ingestion failed: {str(e)}")
            raise
    else:
        logger.info("\n[STAGE 1A] SKIPPED - Using existing Taxi Zone data (skip_zone_ingestion=True)")
        results["zone"] = {"status": "skipped"}

    # ===== STAGE 1B: EXTRACT (NYC Yellow Taxi Trips) =====
    logger.info("\n" + "-" * 70)
    logger.info("[STAGE 1B] EXTRACT - Ingesting NYC Yellow Taxi trip data")
    logger.info("-" * 70)

    try:
        ingest_nyc_flow()
        results["nyc_taxi"] = {"status": "success"}
        logger.info("✓ NYC Yellow Taxi ingestion completed successfully!")

    except Exception as e:
        logger.error(f"✗ NYC Yellow Taxi ingestion failed: {str(e)}")
        raise

    # ===== STAGE 1C: EXTRACT (Weather) =====
    # ⚠️  Weather data stabil - jalankan 1x per hari saja
    if not skip_weather_ingestion:
        logger.info("\n" + "-" * 70)
        logger.info("[STAGE 1C] EXTRACT - Ingesting weather data from Open-Meteo API")
        logger.info("-" * 70)

        try:
            weather_result = ingest_weather_flow(
                start_date=start_date,
                end_date=end_date,
                output_dir=weather_output_dir,
            )
            results["weather"] = weather_result

            logger.info(f"✓ Weather ingestion completed successfully!")
            logger.info(f"  📊 Date range: {weather_result['date_range']}")
            logger.info(f"  ⏱️  Hourly records: {weather_result['records_hourly']}")
            logger.info(f"  📈 Daily aggregates: {weather_result['records_daily']}")

        except Exception as e:
            logger.error(f"✗ Weather ingestion failed: {str(e)}")
            raise
    else:
        logger.info("\n[STAGE 1C] SKIPPED - Using existing weather data (skip_weather_ingestion=True)")
        results["weather"] = {"status": "skipped"}

    # ===== STAGE 2: LOAD (ELT) - Load raw data into DuckDB staging =====
    logger.info("\n" + "-" * 70)
    logger.info("[STAGE 2] LOAD - Loading raw TLC and Weather into DuckDB (staging)")
    logger.info("-" * 70)

    try:
        # Determine TLC raw files (allow override via raw_tlc_files param)
        from pathlib import Path

        if raw_tlc_files:
            tlc_files = raw_tlc_files
        else:
            tlc_dir = Path("data/raw/tlc")
            tlc_files = [str(p) for p in sorted(tlc_dir.glob("yellow_tripdata_*.parquet"))]

        if not tlc_files:
            logger.warning("⚠️  No TLC raw files found for loading to DuckDB (skipping TLC load)")
        else:
            load_result = load_tlc_to_duckdb(db_path, tlc_files)
            results["tlc_load"] = load_result
            logger.info(f"✓ TLC loaded to DuckDB: {load_result.get('rows', '?'):,} rows")

        # Weather raw files from ingest step
        # Only daily weather is needed for weather_transformed and fact_trips.
        weather_files_to_load = []
        if 'weather' in results and isinstance(results['weather'], dict):
            wr = results['weather']
            if wr.get('daily_file'):
                weather_files_to_load.append(wr['daily_file'])

        # fallback: look into data/raw/weather and prefer daily files only
        if not weather_files_to_load:
            wdir = Path('data/raw/weather')
            if wdir.exists():
                weather_files_to_load = [str(p) for p in sorted(wdir.glob('weather_daily_*.parquet'))]

        if weather_files_to_load:
            weather_load_result = load_weather_to_duckdb(db_path, weather_files_to_load)
            results['weather_load'] = weather_load_result
            logger.info(f"✓ Weather loaded to DuckDB: {weather_load_result.get('rows', '?'):,} rows")
        else:
            logger.info("⏭️  No weather raw files found to load into DuckDB")

    except Exception as e:
        logger.error(f"✗ Load stage failed: {str(e)}")
        raise

    # ===== STAGE 3: TRANSFORM (ELT) - Run SQL transforms inside DuckDB =====
    logger.info("\n" + "-" * 70)
    logger.info("[STAGE 3] TRANSFORM - Running SQL transforms in DuckDB")
    logger.info("-" * 70)

    try:
        # Transform weather first (depends on weather_raw)
        try:
            weather_transform_result = transform_weather_in_duckdb(db_path)
            results['weather_transform'] = weather_transform_result
            logger.info(f"✓ Weather transformed in DB: {weather_transform_result.get('rows_after', '?'):,} rows")
        except Exception as e_w:
            logger.warning(f"⚠️  Weather transform skipped/failed: {str(e_w)}")

        # Transform TLC (creates tlc_cleaned and exports parquet)
        tlc_transform_result = transform_tlc_in_duckdb(db_path)
        results['tlc_transform'] = tlc_transform_result
        logger.info(f"✓ TLC transformed in DB: {tlc_transform_result.get('rows_after', '?'):,} rows")

    except Exception as e:
        logger.error(f"✗ Transform stage failed: {str(e)}")
        raise

    # ===== STAGE 4: CREATE STAR SCHEMA (ELT Load + JOIN stages) =====
    logger.info("\n" + "-" * 70)
    logger.info("[STAGE 4] LOAD - Creating Star Schema (fact_trips + dimensions)")
    logger.info("         ↳ This stage JOINs cleaned TLC ← weather_transformed ← dimensions")
    logger.info("-" * 70)

    try:
        schema_result = create_star_schema(db_path, models_dir="models")
        results['star_schema'] = schema_result
        
        logger.info(f"\n✓ Star schema created successfully!")
        for model_name, model_data in schema_result['results'].items():
            rows = model_data.get('rows', '?')
            logger.info(f"  - {model_name}: {rows:,} rows")

        # Generate and log summary
        summary = generate_schema_summary(db_path)
        results['schema_summary'] = summary
        
        logger.info(f"\n📊 Schema Summary:")
        logger.info(f"  Total trips (fact_trips): {summary.get('total_trips', '?'):,}")
        if 'revenue_stats' in summary:
            rev = summary['revenue_stats']
            logger.info(f"  Total revenue: ${rev.get('total', 0):,.2f}")
            logger.info(f"  Avg fare: ${rev.get('avg_fare', 0):.2f}")
            
    except Exception as e:
        logger.error(f"✗ Star schema creation failed: {str(e)}")
        raise

    # ===== STAGE 5: FINALIZE (Ready for Analysis) =====
    logger.info("\n" + "-" * 70)
    logger.info("[STAGE 5] FINALIZE - ELT Pipeline Complete")
    logger.info("-" * 70)

    logger.info(f"✓ All intermediate and final data saved:")
    logger.info(f"  📁 Intermediate parquet: data/intermediate/cleaned.parquet")
    logger.info(f"  📊 Final DuckDB: {db_path}")
    logger.info(f"  📋 Available tables in DuckDB:")
    logger.info(f"      - tlc_raw (staging, {results.get('tlc_load', {}).get('rows', '?'):,} rows)")
    logger.info(f"      - tlc_cleaned (cleaned, {results.get('tlc_transform', {}).get('rows_after', '?'):,} rows)")
    logger.info(f"      - weather_raw (staging)")
    logger.info(f"      - weather_transformed (cleaned, {results.get('weather_transform', {}).get('rows_after', '?'):,} rows)")
    logger.info(f"      - dim_time (dimension)")
    logger.info(f"      - dim_location (dimension)")
    logger.info(f"      - dim_weather (dimension)")
    logger.info(f"      - fact_trips (fact table with TLC-weather JOIN, {summary.get('total_trips', '?'):,} rows)")

    logger.info("\n" + "=" * 70)
    logger.info("✅ ELT PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 70)

    logger.info("\n📋 PIPELINE SUMMARY (ELT Architecture):")
    logger.info("  ✓ Stage 1 (Extract): Zone, NYC Taxi, Weather data ingested")
    logger.info("  ✓ Stage 2 (Load): Raw data loaded to DuckDB staging tables")
    logger.info("  ✓ Stage 3 (Transform): SQL transforms applied (anomaly filtering, features)")
    logger.info("  ✓ Stage 4 (Load): Star schema created with fact_trips (TLC-weather JOIN)")
    logger.info("\n🎯 Deliverables:")
    logger.info(f"  ✓ Intermediate parquet: data/intermediate/cleaned.parquet (TLC cleaned data)")
    logger.info(f"  ✓ Final DuckDB: {db_path} (star schema ready for analysis/ML)")
    logger.info("\n🔍 Ready for:")
    logger.info("  → Data Analyst queries on fact_trips with weather context")
    logger.info("  → ML Engineer feature engineering from fact_trips")
    logger.info("  → Dashboard/BI tools connecting to DuckDB")

    logger.info("\n" + "=" * 70)

    return {"ingestion": results}


# ===================================================================
# OTOMASI / SCHEDULING - OPTIMIZED FOR DAILY RUNS
# ===================================================================
# 
# SCHEDULE YANG DIPILIH: Sekali sehari (paling ringan & efisien)
# - TLC: 1x sehari (00:00 / midnight)
# - Weather: 1x sehari (sama waktu)
# - Zone: 1x setup awal (jarang berubah)
#
# Kelebihan:
# ✓ Ringan di resource (hanya 1x per hari)
# ✓ Data TLC selalu fresh (daily update)
# ✓ Weather consistent
# ✓ Perfect untuk production stable
#
# Cara jalankan:
#
# 1. MANUAL (Testing - Run Now):
#    python pipeline/prefect_flow.py
#
# 2. PRODUCTION - Daily at 00:00 (RECOMMENDED):
#    python pipeline/prefect_flow.py --mode production
#
# 3. DEVELOPMENT - Daily at 02:00 (staggered):
#    python pipeline/prefect_flow.py --mode dev
#
# ===================================================================


def setup_production_schedule():
    """Setup production schedule: Daily at 00:00 (midnight) - Lightweight & Stable."""
    from prefect.schedules import CronSchedule
    
    # Daily schedule at 00:00 America/New_York
    daily_schedule = CronSchedule(cron="0 0 * * *", timezone="America/New_York")
    
    logger = get_run_logger()
    logger.info("📅 Production Schedule Setup (DAILY):")
    logger.info("  Schedule: TLC + Weather + Zone Pipeline")
    logger.info("    ↳ Frequency: 1x per day at 00:00 (midnight)")
    logger.info("    ↳ Timezone: America/New_York")
    logger.info("    ↳ Command: main_pipeline(skip_zone_ingestion=True, skip_weather_ingestion=False)")
    logger.info("\n  Benefit:")
    logger.info("    ✓ Light on resources (1 run/day)")
    logger.info("    ✓ Fresh TLC data daily")
    logger.info("    ✓ Fresh weather data daily")
    logger.info("    ✓ Stable production schedule")


if __name__ == "__main__":
    import sys
    import logging
    
    # Setup basic logging for non-flow context
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger_main = logging.getLogger(__name__)
    
    # Parse command line arguments untuk scheduling mode
    mode = "manual"  # default: manual
    if len(sys.argv) > 1:
        if "--mode" in sys.argv:
            idx = sys.argv.index("--mode")
            if idx + 1 < len(sys.argv):
                mode = sys.argv[idx + 1]
    
    if mode == "production":
        # Production: Daily at 00:00 (midnight) America/New_York timezone
        main_pipeline.serve(
            name="taxi-elt-daily-production",
            cron="0 0 * * *",  # Every day at 00:00 (midnight)
            parameters={
                "skip_zone_ingestion": True,
                "skip_weather_ingestion": False,
            }
        )
    
    elif mode == "dev":
        # Development: Daily at 02:00 (staggered from production)
        main_pipeline.serve(
            name="taxi-elt-daily-dev",
            cron="0 2 * * *",  # Every day at 02:00 (2 AM)
            parameters={
                "skip_zone_ingestion": True,
                "skip_weather_ingestion": False,
            }
        )
    
    else:
        # Manual run (setiap kali script dijalankan)
        result = main_pipeline(
            skip_zone_ingestion=False,  # First time: ingest zone
            skip_weather_ingestion=False,
        )
        print("\n" + "=" * 70)
        print("✅ PIPELINE RUN COMPLETED")
        print("=" * 70)
        print(f"Result: {result}")
        print("\nTo setup automatic daily scheduling:")
        print("  python pipeline/prefect_flow.py --mode production  # Daily at 00:00")
        print("  python pipeline/prefect_flow.py --mode dev         # Daily at 02:00")
        print("=" * 70)
