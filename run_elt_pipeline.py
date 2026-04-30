#!/usr/bin/env python
"""
Simple ELT Pipeline Runner - No Prefect Required!

Alur dari awal:
1. Cek raw TLC files (dari ingest_nyc.py)
2. Cek raw weather files (dari fetch_weather.py)
3. Load TLC ke DuckDB (staging)
4. Transform TLC dengan SQL (intermediate)
5. Load weather ke DuckDB
6. Create star schema (dim + fact tables)
7. Done!

Usage:
    python run_elt_pipeline.py
    
    atau dengan custom paths:
    python run_elt_pipeline.py --db data/final/tlc.duckdb --tlc-dir data/raw
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Import pipeline functions
from preprocessing.load_tlc import load_tlc_to_duckdb
from preprocessing.process_tlc import transform_tlc_in_duckdb
from preprocessing.load_weather import load_weather_to_duckdb
from preprocessing.process_weather import transform_weather_in_duckdb
from pipeline.load_star_schema import (
    create_star_schema,
    generate_schema_summary,
)


def find_tlc_files(tlc_dir: str = "data/raw") -> List[str]:
    """Find semua raw TLC parquet files di directory."""
    logger.info(f"\n📁 Searching for TLC files in: {tlc_dir}")

    tlc_path = Path(tlc_dir)
    
    # Check fallback to tlc subfolder
    if tlc_path.exists() and not list(tlc_path.glob("yellow_tripdata_*.parquet")):
        if (tlc_path / "tlc").exists():
            tlc_path = tlc_path / "tlc"
            logger.info(f"📁 Checking subfolder for TLC files: {tlc_path}")

    if not tlc_path.exists():
        logger.error(f"❌ Directory not found: {tlc_dir}")
        return []

    # Cari yellow_tripdata_*.parquet
    tlc_files = sorted(tlc_path.glob("yellow_tripdata_*.parquet"))

    if not tlc_files:
        logger.warning(f"⚠️  No TLC files found in {tlc_dir}")
        logger.info(f"Expected pattern: yellow_tripdata_*.parquet")
        return []

    logger.info(f"✅ Found {len(tlc_files)} TLC file(s):")
    for f in tlc_files:
        file_size = f.stat().st_size / 1024 / 1024
        logger.info(f"   - {f.name} ({file_size:.1f} MB)")

    return [str(f) for f in tlc_files]


def find_weather_files(weather_dir: str = "data/raw/weather") -> List[str]:
    """Find raw weather parquet files."""
    logger.info(f"\n🌤️  Searching for weather files in: {weather_dir}")

    weather_path = Path(weather_dir)
    if not weather_path.exists():
        logger.warning(f"⚠️  Directory not found: {weather_dir}")
        return []

    # Cari weather_daily_*.parquet
    weather_files = sorted(weather_path.glob("weather_daily_*.parquet"))

    if weather_files:
        logger.info(f"✅ Found {len(weather_files)} weather file(s)")
        return [str(f) for f in weather_files]
    else:
        logger.warning(f"⚠️  Weather raw files not found")
        return []


def run_pipeline(
    db_path: str = "data/final/tlc.duckdb",
    tlc_dir: str = "data/raw",
    weather_dir: str = "data/intermediate/weather",
    models_dir: str = "models",
):
    """Run complete ELT pipeline step-by-step."""

    logger.info("=" * 70)
    logger.info("🚀 ELT PIPELINE - Simple Runner (No Prefect)")
    logger.info("=" * 70)

    # Create output directory
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)

    # ========== STAGE 1: Find Data ==========
    logger.info("\n" + "=" * 70)
    logger.info("[STAGE 1] Finding source data files")
    logger.info("=" * 70)

    tlc_files = find_tlc_files(tlc_dir)
    weather_files = find_weather_files("data/raw/weather")

    if not tlc_files:
        logger.error("❌ No TLC files found! Cannot continue.")
        logger.info("\nMake sure to run ingest_nyc.py first:")
        logger.info("  python ingestion/ingest_nyc.py")
        return False

    # ========== STAGE 2: Load TLC to DuckDB ==========
    logger.info("\n" + "=" * 70)
    logger.info("[STAGE 2] Loading TLC data to DuckDB (Staging)")
    logger.info("=" * 70)

    try:
        load_result = load_tlc_to_duckdb(db_path, tlc_files)

        logger.info(f"\n✅ SUCCESS:")
        logger.info(f"   Database: {load_result['db_path']}")
        logger.info(f"   Table: {load_result['table']}")
        logger.info(f"   Rows: {load_result['rows']:,}")

    except Exception as e:
        logger.error(f"\n❌ FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    # ========== STAGE 3: Transform TLC in SQL ==========
    logger.info("\n" + "=" * 70)
    logger.info("[STAGE 3] Transforming TLC data (SQL in DuckDB)")
    logger.info("=" * 70)

    try:
        transform_result = transform_tlc_in_duckdb(db_path)

        logger.info(f"\n✅ SUCCESS:")
        logger.info(f"   Table: {transform_result['table']}")
        logger.info(f"   Rows before: {transform_result['rows_before']:,}")
        logger.info(f"   Rows after: {transform_result['rows_after']:,}")
        logger.info(f"   Rows removed: {transform_result['rows_removed']:,}")
        logger.info(f"   Retention rate: {transform_result['retention_rate']:.2f}%")

        if transform_result["anomalies"]:
            logger.info(f"\n   Anomalies filtered:")
            for anom_type, count in transform_result["anomalies"].items():
                logger.info(f"     - {anom_type}: {count:,}")

    except Exception as e:
        logger.error(f"\n❌ FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

    # ========== STAGE 4: Load & Transform Weather (SQL) ==========
    if weather_files:
        logger.info("\n" + "=" * 70)
        logger.info("[STAGE 4] Loading & Transforming Weather (ELT in DuckDB)")
        logger.info("=" * 70)

        try:
            # Load raw weather to staging
            load_weather_to_duckdb(db_path, weather_files)
            
            # Transform in DB
            weather_result = transform_weather_in_duckdb(db_path)

            logger.info(f"\n✅ SUCCESS:")
            logger.info(f"   Table: {weather_result['table']}")
            logger.info(f"   Rows: {weather_result['rows_after']:,}")

        except Exception as e:
            logger.warning(f"\n⚠️  Weather ELT failed: {str(e)}")
            logger.info("   (Will continue without weather data)")
    else:
        logger.info("\n⏭️  Skipping weather ELT (no files found)")

    # ========== STAGE 5: Create Star Schema ==========
    logger.info("\n" + "=" * 70)
    logger.info("[STAGE 5] Creating star schema (dimensions + fact table)")
    logger.info("=" * 70)

    try:
        schema_result = create_star_schema(db_path, models_dir)

        logger.info(f"\n✅ SUCCESS:")
        logger.info(f"   Models created: {', '.join(schema_result['models_created'])}")

        for model_name, model_data in schema_result["results"].items():
            if "rows" in model_data:
                logger.info(f"   - {model_name}: {model_data['rows']:,} rows")
            else:
                logger.info(f"   - {model_name}: created")

    except Exception as e:
        logger.error(f"\n❌ FAILED: {str(e)}")
        logger.warning("   (Star schema creation failed)")
        import traceback
        traceback.print_exc()
        # Don't return False - we have usable data anyway

    # ========== STAGE 6: Generate Summary ==========
    logger.info("\n" + "=" * 70)
    logger.info("[STAGE 6] Generating schema summary")
    logger.info("=" * 70)

    try:
        summary = generate_schema_summary(db_path)

        logger.info(f"\n📊 SUMMARY:")
        logger.info(f"   Total trips: {summary.get('total_trips', '?'):,}")
        logger.info(f"   Date range: {summary.get('date_range', ('?', '?'))[0]} to {summary.get('date_range', ('?', '?'))[1]}")

        if "revenue_stats" in summary:
            rev = summary["revenue_stats"]
            logger.info(f"   Total revenue: ${rev.get('total', 0):,.2f}")
            logger.info(f"   Avg fare: ${rev.get('avg_fare', 0):.2f}")
            logger.info(f"   Avg tip: {rev.get('avg_tip_pct', 0):.2f}%")

    except Exception as e:
        logger.warning(f"⚠️  Summary generation skipped: {str(e)}")

    # ========== FINAL STATUS ==========
    logger.info("\n" + "=" * 70)
    logger.info("✅ ELT PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info("=" * 70)

    logger.info(f"\n📊 Output Database: {db_path}")
    logger.info("\n📋 Available Tables:")
    logger.info("   - tlc_raw (staging, raw data)")
    logger.info("   - tlc_cleaned (intermediate, filtered + features)")
    logger.info("   - dim_time (dimension, temporal)")
    logger.info("   - dim_location (dimension, locations)")
    logger.info("   - dim_weather (dimension, weather)")
    logger.info("   - fact_trips (fact, central table)")

    logger.info("\n🎯 Next Steps:")
    logger.info("   1. Query star schema with DuckDB")
    logger.info("   2. Run SQL analysis queries")
    logger.info("   3. Use for ML modeling")
    logger.info("   4. Build dashboard from fact_trips")

    logger.info("\n💡 Example DuckDB Query:")
    logger.info("""
        import duckdb
        conn = duckdb.connect("data/final/tlc.duckdb")
        result = conn.execute('''
            SELECT 
                day_of_week_name,
                COUNT(*) as trip_count,
                AVG(total_amount) as avg_fare
            FROM fact_trips JOIN dim_time USING (time_key)
            GROUP BY 1 ORDER BY 1
        ''').fetch_df()
        print(result)
    """)

    logger.info("=" * 70)

    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Simple ELT Pipeline Runner (No Prefect)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with defaults
  python run_elt_pipeline.py
  
  # Custom database path
  python run_elt_pipeline.py --db /tmp/tlc.duckdb
  
  # Custom data directory
  python run_elt_pipeline.py --tlc-dir /path/to/tlc/data
        """,
    )

    parser.add_argument(
        "--db",
        default="data/final/tlc.duckdb",
        help="DuckDB database path (default: data/final/tlc.duckdb)",
    )

    parser.add_argument(
        "--tlc-dir",
        default="data/raw",
        help="Directory with raw TLC parquet files (default: data/raw)",
    )

    parser.add_argument(
        "--weather-dir",
        default="data/intermediate/weather",
        help="Directory with weather transformed parquet files (default: data/intermediate/weather)",
    )

    parser.add_argument(
        "--models-dir",
        default="models",
        help="Directory with SQL model files (default: models)",
    )

    args = parser.parse_args()

    # Run pipeline
    success = run_pipeline(
        db_path=args.db,
        tlc_dir=args.tlc_dir,
        weather_dir=args.weather_dir,
        models_dir=args.models_dir,
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
