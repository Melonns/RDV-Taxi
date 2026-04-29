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

from datetime import datetime
from typing import Optional

from prefect import flow, get_run_logger

from ingestion.fetch_weather import ingest_weather_flow
from ingestion.ingest_nyc import ingest_nyc_flow
from ingestion.ingest_zone import ingest_zone_lookup_flow
# from preprocessing.preprocessing_flow import preprocessing_weather_flow


@flow(
    name="main_elt_pipeline",
    description="Master ELT pipeline: Zone + NYC Taxi + Weather ingestion dengan otomasi",
)
def main_pipeline(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    weather_output_dir: Optional[str] = None,
) -> dict:
    """Master pipeline orchestrator untuk data ingestion + preprocessing (ETL).

    Menjalankan weather data ingestion dan preprocessing secara otomatis.
    Flow ini bisa di-schedule otomatis via Prefect deployment.

    Flow sequence:
      1. Fetch weather dari Open-Meteo API
      2. Save to raw parquet
      3. Clean anomalies + validate ranges
      4. Transform + add temporal features
      5. Save intermediate parquet (ready untuk model)

    Args:
        start_date: Weather start date (YYYY-MM-DD). Default: 2025-01-01
        end_date: Weather end date (YYYY-MM-DD). Default: 2025-06-30
        weather_output_dir: Output directory untuk weather data

    Returns:
        Dictionary dengan hasil ingestion
    """
    logger = get_run_logger()

    logger.info("=" * 70)
    logger.info("🚀 Starting Main ELT  Pipeline - Zone + NYC Taxi + Weather Ingestion")
    logger.info("=" * 70)
    logger.info(f"⏰ Timestamp: {datetime.now().isoformat()}")

    results = {}

    # ===== STAGE 1A: EXTRACT (Taxi Zone Lookup) =====
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

    # ===== STAGE 2: TRANSFORM (Preprocessing) =====
    # logger.info("\n" + "-" * 70)
    # logger.info("[STAGE 2] TRANSFORM - Cleaning + Feature Engineering")
    # logger.info("-" * 70)

    # try:
    #     preprocessing_result = preprocessing_weather_flow(
    #         raw_hourly_file=weather_result["hourly_file"],
    #         raw_daily_file=weather_result["daily_file"],
    #         output_dir=weather_output_dir,
    #     )
    #     results["preprocessing"] = preprocessing_result

    #     logger.info(f"✓ Weather preprocessing completed successfully!")
    #     logger.info(f"  Hourly transformed: {preprocessing_result['hourly_transformed']['rows']} rows")
    #     logger.info(f"  Daily transformed: {preprocessing_result['daily_transformed']['rows']} rows")

    # except Exception as e:
    #     logger.error(f"✗ Weather preprocessing failed: {str(e)}")
    #     raise

    # ===== STAGE 3: LOAD (Ready) =====
    # logger.info("\n" + "-" * 70)
    # logger.info("[STAGE 3] LOAD - Intermediate data ready for next stages")
    # logger.info("-" * 70)

    # logger.info(f"✓ Intermediate data saved at:")
    # logger.info(f"  📁 {preprocessing_result['hourly_transformed']['output_file']}")
    # logger.info(f"  📁 {preprocessing_result['daily_transformed']['output_file']}")

    logger.info("\n" + "=" * 70)
    logger.info("✅ Main  Pipeline Completed Successfully")
    logger.info("=" * 70)

    return {"ingestion": results}


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
#      --cron "0 0 1 * *" \
#      --apply
#    # Ini akan jalankan flow otomatis setiap tanggal 1 jam 00:00
#
# 3. VIA PREFECT WORKER:
#    prefect worker start
#    # Di terminal lain:
#    prefect deployment build pipeline/prefect_flow.py:main_pipeline --apply


if __name__ == "__main__":
    main_pipeline.serve(name="monthly-master-ingestion", cron="0 0 1 * *")
