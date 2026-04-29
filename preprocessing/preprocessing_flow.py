"""Preprocessing flow: Clean dan Transform weather data dengan Prefect."""

import os
from pathlib import Path
from typing import Optional

from prefect import flow, get_run_logger, task

from preprocessing.clean import clean_raw_data
from preprocessing.transform import transform_data


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
def transform_weather_task(
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


if __name__ == "__main__":
    # Example usage
    hourly_file = "data/raw/weather/weather_hourly_2025-01-01_to_2025-06-30.parquet"
    daily_file = "data/raw/weather/weather_daily_2025-01-01_to_2025-06-30.parquet"

    result = preprocessing_weather_flow(hourly_file, daily_file)

    print("\n" + "=" * 70)
    print("📊 PREPROCESSING SUMMARY")
    print("=" * 70)
    print(f"\n✅ Hourly Transform Output: {result['hourly_transformed']['output_file']}")
    print(f"   Rows: {result['hourly_transformed']['rows']}")
    print(f"\n✅ Daily Transform Output: {result['daily_transformed']['output_file']}")
    print(f"   Rows: {result['daily_transformed']['rows']}")
    print("=" * 70)
