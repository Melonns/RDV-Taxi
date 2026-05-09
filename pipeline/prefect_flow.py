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

from datetime import datetime, timedelta
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
# Batch state management
from pipeline.batch_state import get_next_batch_month, mark_batch_complete, read_state


@flow(
    name="main_elt_pipeline",
    description="Master ELT pipeline: Incremental batch ingestion per bulan dengan otomasi",
)
def main_pipeline(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    weather_output_dir: Optional[str] = None,
    db_path: Optional[str] = None,
    raw_tlc_files: Optional[list] = None,
    skip_zone_ingestion: bool = False,
    skip_weather_ingestion: bool = False,
    skip_state_management: bool = False,
) -> dict:
    """Master pipeline orchestrator dengan INCREMENTAL BATCH ingestion per bulan.

    BATCH INGESTION ARCHITECTURE:
      Setiap execution memproses HANYA 1 bulan (bukan semua 6 bulan sekaligus):
      1. Check state file → tentukan bulan mana yang belum diproses
      2. Jika ada bulan baru → ingest data bulan itu saja
      3. Jika sudah sampai Juni → skip ingestion gracefully (no error)
      4. Transform & load ke star schema (Stages 2-5 tetap berjalan)
      5. Update state file setelah sukses

    STATE MANAGEMENT:
      - Metadata disimpan di: data/pipeline_state.json
      - Track: last_processed_month, last_processed_year, total_batches_processed
      - Allows resumable, idempotent processing

    Args:
        start_date: Weather start date untuk batch. Default: auto-determined dari state
        end_date: Weather end date untuk batch. Default: auto-determined dari state
        weather_output_dir: Output directory untuk weather intermediate files
        db_path: DuckDB database path. Default: data/final/tlc.duckdb
        raw_tlc_files: List of raw TLC parquet file paths (optional)
        skip_zone_ingestion: Skip zone lookup ingestion. Default: False
        skip_weather_ingestion: Skip weather ingestion. Default: False
        skip_state_management: Bypass state checking (manual mode). Default: False

    Returns:
        Dictionary dengan hasil ingestion
    """
    logger = get_run_logger()

    # Set defaults
    if db_path is None:
        db_path = "./data/final/tlc.duckdb"

    logger.info("=" * 70)
    logger.info("🚀 Starting Main ELT Pipeline - INCREMENTAL BATCH INGESTION")
    logger.info("=" * 70)
    logger.info(f"⏰ Timestamp: {datetime.now().isoformat()}")
    logger.info(f"📊 DuckDB: {db_path}")

    results = {}
    batch_info = None
    
    # ===== CHECK STATE & DETERMINE BATCH MONTH =====
    if not skip_state_management:
        logger.info("\n" + "-" * 70)
        logger.info("[STATE CHECK] Determining next batch month to process")
        logger.info("-" * 70)
        
        batch_info = get_next_batch_month()
        logger.info(f"  Status: {batch_info['reason']}")
        logger.info(f"  Total batches completed: {batch_info.get('total_processed', 0)}")
        
        if not batch_info["is_valid"]:
            # All batches completed - skip ingestion gracefully
            logger.info("\n✓ All monthly batches have been processed successfully!")
            logger.info("  Pipeline will now skip ingestion and proceed to analytics stages.")
            skip_weather_ingestion = True
            skip_zone_ingestion = True
        else:
            # Valid batch - determine date range for this month
            batch_month = batch_info["month"]
            batch_year = batch_info["year"]
            
            # Calculate start and end date untuk bulan ini
            start_date = f"{batch_year}-{batch_month:02d}-01"
            
            # End date adalah hari terakhir bulan ini
            if batch_month == 12:
                next_month_start = datetime(batch_year + 1, 1, 1)
            else:
                next_month_start = datetime(batch_year, batch_month + 1, 1)
            end_date = (next_month_start - timedelta(days=1)).strftime("%Y-%m-%d")
            
            logger.info(f"\n  Batch month: {batch_month:02d}/{batch_year}")
            logger.info(f"  Date range: {start_date} to {end_date}")
            logger.info(f"  → Will ingest data ONLY for this month")

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
        if skip_state_management or (batch_info and batch_info["is_valid"]):
            # Skip zone untuk frequent batch runs (zone data jarang berubah)
            logger.info("\n[STAGE 1A] SKIPPED - Using existing Taxi Zone data (batch run)")
        else:
            # Skip karena no new batches
            logger.info("\n[STAGE 1A] SKIPPED - Using existing Taxi Zone data (no new batches)")
        results["zone"] = {"status": "skipped"}

    # ===== STAGE 1B: EXTRACT (NYC Yellow Taxi Trips) =====
    if not (skip_weather_ingestion and skip_zone_ingestion):
        # Hanya jalankan jika ada batch baru untuk diingest
        logger.info("\n" + "-" * 70)
        logger.info("[STAGE 1B] EXTRACT - Ingesting NYC Yellow Taxi trip data")
        logger.info("-" * 70)

        try:
            # If batch_info exists and valid, pass target month/year to ingest only that month
            if batch_info and batch_info.get("is_valid"):
                target_month = batch_info["month"]
                target_year = batch_info["year"]
                logger.info(f"  → Ingesting only TLC month: {target_month:02d}/{target_year}")
                downloaded_files = ingest_nyc_flow(target_month, target_year)
                # Inform downstream stages which raw TLC files to load
                raw_tlc_files = downloaded_files
            else:
                downloaded_files = ingest_nyc_flow()

            results["nyc_taxi"] = {"status": "success", "files": downloaded_files}
            logger.info("✓ NYC Yellow Taxi ingestion completed successfully!")

        except Exception as e:
            logger.error(f"✗ NYC Yellow Taxi ingestion failed: {str(e)}")
            raise
    else:
        logger.info("\n[STAGE 1B] SKIPPED - Using existing NYC Taxi data (no new batches)")
        results["nyc_taxi"] = {"status": "skipped"}

    # ===== STAGE 1C: EXTRACT (Weather) =====
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
        logger.info("\n[STAGE 1C] SKIPPED - Using existing weather data (no new batch)")
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

    # ===== UPDATE STATE (after successful run) =====
    if not skip_state_management and batch_info and batch_info["is_valid"]:
        try:
            mark_batch_complete(batch_info["month"], batch_info["year"])
            logger.info(f"\n✓ State updated: Batch {batch_info['month']:02d}/{batch_info['year']} marked as complete")
            logger.info(f"  Total batches processed so far: {batch_info.get('total_processed', 0) + 1}")
            logger.info(f"  Next run will process: {batch_info['month'] + 1 if batch_info['month'] < 12 else 1}/{batch_info['year'] if batch_info['month'] < 12 else batch_info['year'] + 1}")
        except Exception as e:
            logger.warning(f"⚠️  Failed to update state: {str(e)}")

    logger.info("\n📋 PIPELINE SUMMARY (Incremental Batch ELT Architecture):")
    logger.info("  ✓ Stage 1 (Extract): Batch data extracted (zone/taxi/weather as needed)")
    logger.info("  ✓ Stage 2 (Load): Raw data loaded to DuckDB staging tables")
    logger.info("  ✓ Stage 3 (Transform): SQL transforms applied (anomaly filtering, features)")
    logger.info("  ✓ Stage 4 (Load): Star schema created with fact_trips (TLC-weather JOIN)")
    logger.info("  ✓ Stage 5 (Finalize): Ready for analysis/ML")
    
    logger.info("\n🔍 Ready for:")
    logger.info("  → Data Analyst queries on fact_trips with weather context")
    logger.info("  → ML Engineer feature engineering from fact_trips")
    logger.info("  → Dashboard/BI tools connecting to DuckDB")

    logger.info("\n" + "=" * 70)

    return {"ingestion": results}


# ===================================================================
# OTOMASI / SCHEDULING - OPTIMIZED FOR INCREMENTAL BATCH INGESTION
# ===================================================================
# 
# SCHEDULE YANG DIPILIH: Setiap 5-10 menit (untuk demo/development)
# - Batch per bulan (bukan all at once)
# - State-driven: otomatis determine bulan mana yang belum diproses
# - Graceful handling: jika semua bulan sudah processed, skip ingestion
# - Production: bisa diubah ke daily/monthly schedule sesuai kebutuhan
#
# Kelebihan batch incremental:
# ✓ Simulasi realistic production behavior
# ✓ Resumable: jika pipeline gagal, next run lanjut dari batch yang sama
# ✓ Flexible: bisa adjust scheduling sesuai data availability
# ✓ State-tracked: mudah debug dan audit
#
# Cara jalankan:
#
# 1. DEMO/DEVELOPMENT - Every 5 minutes (development):
#    python pipeline/prefect_flow.py --mode demo
#
# 2. PRODUCTION - Daily at 00:00 (realistic production):
#    python pipeline/prefect_flow.py --mode production
#
# 3. MANUAL - Run once now:
#    python pipeline/prefect_flow.py
#
# ===================================================================


def setup_demo_schedule():
    """Setup demo schedule: Every 5 minutes - untuk testing batch ingestion."""
    logger = get_run_logger()
    logger.info("\n📅 Demo Schedule Setup (BATCH INGESTION - Every 5 min):")
    logger.info("  Schedule: Incremental monthly batch ingestion")
    logger.info("    ↳ Frequency: Every 5 minutes")
    logger.info("    ↳ Mode: State-driven batch processing")
    logger.info("    ↳ Timezone: America/New_York")
    logger.info("\n  Behavior:")
    logger.info("    ✓ Each run processes 1 month (Jan→Feb→Mar→...→Jun)")
    logger.info("    ✓ State file tracks last_processed_month")
    logger.info("    ✓ When June reached, gracefully skip ingestion (no error)")
    logger.info("    ✓ Perfect for demo & testing")


def setup_production_schedule():
    """Setup production schedule: Daily at 00:00 dengan batch ingestion."""
    logger = get_run_logger()
    logger.info("\n📅 Production Schedule Setup (BATCH INGESTION - Daily):")
    logger.info("  Schedule: TLC + Weather + Zone Pipeline")
    logger.info("    ↳ Frequency: Daily at 00:00 (midnight)")
    logger.info("    ↳ Mode: State-driven batch processing")
    logger.info("    ↳ Timezone: America/New_York")
    logger.info("\n  Behavior:")
    logger.info("    ✓ Each day processes next month's data (if available)")
    logger.info("    ✓ After ~6 days, all months (Jan-Jun) will be processed")
    logger.info("    ✓ Pipeline continues but skips ingestion gracefully")
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
    
    if mode == "demo":
        # Demo: Every 5 minutes (untuk testing batch ingestion)
        logger_main.info("\n" + "=" * 70)
        logger_main.info("🚀 LAUNCHING DEMO MODE - Batch ingestion every 5 minutes")
        logger_main.info("=" * 70)
        logger_main.info("\nSchedule: ✓ Every 5 minutes")
        logger_main.info("Behavior: ✓ Process 1 month per run (Jan→Feb→Mar→...→Jun)")
        logger_main.info("State:    ✓ Tracked in data/pipeline_state.json")
        logger_main.info("\nTo stop: Press Ctrl+C\n")
        
        main_pipeline.serve(
            name="taxi-elt-batch-demo",
            cron="*/5 * * * *",  # Every 5 minutes
            parameters={
                "skip_zone_ingestion": True,  # Zone not needed for frequent runs
                "skip_weather_ingestion": False,
                "skip_state_management": False,  # Enable state tracking
            }
        )
    
    elif mode == "production":
        # Production: Daily at 00:00 (midnight) dengan batch ingestion
        logger_main.info("\n" + "=" * 70)
        logger_main.info("🚀 LAUNCHING PRODUCTION MODE - Batch ingestion daily")
        logger_main.info("=" * 70)
        logger_main.info("\nSchedule: ✓ Daily at 00:00 (midnight)")
        logger_main.info("Behavior: ✓ Process 1 month per day (Jan→Feb→Mar→...→Jun)")
        logger_main.info("State:    ✓ Tracked in data/pipeline_state.json")
        logger_main.info("Zone:     ✓ Already ingested (skipped for efficiency)")
        logger_main.info("\nAfter ~6 days, all months will be processed.\n")
        
        main_pipeline.serve(
            name="taxi-elt-batch-production",
            cron="0 0 * * *",  # Every day at 00:00 (midnight)
            parameters={
                "skip_zone_ingestion": True,
                "skip_weather_ingestion": False,
                "skip_state_management": False,  # Enable state tracking
            }
        )
    
    else:
        # Manual run (single execution)
        logger_main.info("\n" + "=" * 70)
        logger_main.info("🚀 MANUAL RUN - Single Batch Execution")
        logger_main.info("=" * 70)
        
        result = main_pipeline(
            skip_zone_ingestion=False,  # First time: ingest zone
            skip_weather_ingestion=False,
            skip_state_management=False,  # Enable state tracking
        )
        
        logger_main.info("\n" + "=" * 70)
        logger_main.info("✅ BATCH EXECUTION COMPLETED")
        logger_main.info("=" * 70)
        logger_main.info(f"Result: {result}")
        
        logger_main.info("\nTo setup automatic batch scheduling:")
        logger_main.info("  python pipeline/prefect_flow.py --mode demo       # Every 5 min (testing)")
        logger_main.info("  python pipeline/prefect_flow.py --mode production # Daily at 00:00 (prod)")
        
        # Show current state
        try:
            current_state = read_state()
            logger_main.info("\nCurrent state:")
            logger_main.info(f"  Last processed: {current_state.get('last_processed_month'):02d}/{current_state.get('last_processed_year')}")
            logger_main.info(f"  Total batches: {current_state.get('total_batches_processed')}")
        except Exception as e:
            logger_main.warning(f"Could not read state: {e}")
        
        logger_main.info("\n" + "=" * 70)
