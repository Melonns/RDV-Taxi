"""Weather data cleaning and anomaly detection.

Handles:
- Missing values (forward fill, drop)
- Outlier detection (temperature, precipitation bounds)
- Data type validation
- Null records removal
"""

import logging
from pathlib import Path
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def validate_weather_ranges(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """Validate weather data within realistic ranges.

    Args:
        df: Weather dataframe

    Returns:
        Tuple of (cleaned_df, anomaly_report)
    """
    report = {"total_rows_before": len(df), "anomalies": {}}

    original_len = len(df)

    # Temperature validation: -40°C to 50°C (realistic for NYC)
    temp_cols = [c for c in df.columns if "temperature" in c.lower()]
    if temp_cols:
        for col in temp_cols:
            invalid_temp = df[(df[col] < -40) | (df[col] > 50)].shape[0]
            if invalid_temp > 0:
                report["anomalies"][f"{col}_invalid"] = invalid_temp
                df = df[(df[col] >= -40) & (df[col] <= 50)]

    # Precipitation: can't be negative, cap at 500mm/day (extreme)
    precip_cols = [c for c in df.columns if "precipitation" in c.lower()]
    if precip_cols:
        for col in precip_cols:
            invalid_precip = df[df[col] < 0].shape[0]
            if invalid_precip > 0:
                report["anomalies"][f"{col}_negative"] = invalid_precip
                df[col] = df[col].clip(lower=0)

            extreme_precip = df[df[col] > 500].shape[0]
            if extreme_precip > 0:
                report["anomalies"][f"{col}_extreme"] = extreme_precip
                df = df[df[col] <= 500]

    # Wind speed: should be >= 0, cap at 200 km/h (extreme hurricane)
    wind_cols = [c for c in df.columns if "wind_speed" in c.lower()]
    if wind_cols:
        for col in wind_cols:
            invalid_wind = df[df[col] < 0].shape[0]
            if invalid_wind > 0:
                report["anomalies"][f"{col}_negative"] = invalid_wind
                df[col] = df[col].clip(lower=0)

            extreme_wind = df[df[col] > 200].shape[0]
            if extreme_wind > 0:
                report["anomalies"][f"{col}_extreme"] = extreme_wind
                df = df[df[col] <= 200]

    # Humidity: 0-100%
    humidity_cols = [c for c in df.columns if "humidity" in c.lower()]
    if humidity_cols:
        for col in humidity_cols:
            df[col] = df[col].clip(0, 100)

    rows_removed = original_len - len(df)
    report["rows_removed"] = rows_removed
    report["total_rows_after"] = len(df)

    return df, report


def clean_raw_data(input_path: str, output_path: str) -> dict:
    """Clean raw weather parquet data.

    Args:
        input_path: Path to raw weather parquet file
        output_path: Path to save cleaned parquet file

    Returns:
        Dictionary with cleaning report
    """
    input_file = Path(input_path)
    output_file = Path(output_path)

    logger.info(f"Loading raw data from {input_file}")
    df = pd.read_parquet(input_file)

    logger.info(f"Initial record count: {len(df)}")

    # Handle missing values
    missing_before = df.isnull().sum().sum()
    logger.info(f"Missing values before: {missing_before}")

    # Forward fill for time-series weather data
    numeric_cols = df.select_dtypes(include=["float64", "int64"]).columns
    df[numeric_cols] = df[numeric_cols].fillna(method="ffill", limit=5)

    # Drop rows with remaining nulls
    df = df.dropna()

    missing_after = df.isnull().sum().sum()
    logger.info(f"Missing values after: {missing_after}")

    # Validate ranges
    df, anomaly_report = validate_weather_ranges(df)

    # Log anomalies
    logger.info("\n📊 Cleaning Report:")
    logger.info(f"  Total rows before: {anomaly_report['total_rows_before']}")
    logger.info(f"  Rows removed: {anomaly_report['rows_removed']}")
    logger.info(f"  Total rows after: {anomaly_report['total_rows_after']}")

    if anomaly_report["anomalies"]:
        logger.info("  Anomalies detected:")
        for anomaly_type, count in anomaly_report["anomalies"].items():
            logger.info(f"    - {anomaly_type}: {count}")

    # Save cleaned data
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_file, index=False, compression="snappy")

    logger.info(f"Cleaned data saved to {output_file}")
    logger.info(f"Output file size: {output_file.stat().st_size / 1024 / 1024:.2f} MB")

    return {
        "input_file": str(input_file),
        "output_file": str(output_file),
        "report": anomaly_report,
    }
