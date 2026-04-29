"""Data transformation: feature engineering dan weather joining.

Handles:
- Derived columns (hour, day_of_week, is_weekend, is_peak_hour)
- Join dengan TLC data per jam
- Feature engineering untuk ML
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def add_temporal_features(df: pd.DataFrame, datetime_col: str = "datetime") -> pd.DataFrame:
    """Add temporal features from datetime column.

    Args:
        df: DataFrame dengan datetime column
        datetime_col: Nama kolom datetime

    Returns:
        DataFrame dengan temporal features
    """
    if datetime_col not in df.columns:
        logger.warning(f"Column {datetime_col} not found")
        return df

    df["date"] = pd.to_datetime(df[datetime_col]).dt.date
    df["hour"] = pd.to_datetime(df[datetime_col]).dt.hour
    df["day_of_week"] = pd.to_datetime(df[datetime_col]).dt.day_name()
    df["is_weekend"] = pd.to_datetime(df[datetime_col]).dt.dayofweek >= 5
    df["is_peak_hour"] = df["hour"].isin([7, 8, 9, 17, 18, 19])

    logger.info("✓ Temporal features added")
    return df


def transform_data(
    input_path: str,
    output_path: str,
    add_features: bool = True,
) -> dict:
    """Transform cleaned weather data dengan feature engineering.

    Args:
        input_path: Path to cleaned parquet
        output_path: Path to save transformed parquet
        add_features: Whether to add temporal features

    Returns:
        Dictionary dengan transform report
    """
    input_file = Path(input_path)
    output_file = Path(output_path)

    logger.info(f"Loading cleaned data from {input_file}")
    df = pd.read_parquet(input_file)

    logger.info(f"Initial shape: {df.shape}")

    # Add temporal features
    if add_features:
        df = add_temporal_features(df, "datetime")

    # Cast data types
    if "weathercode" in df.columns:
        df["weathercode"] = df["weathercode"].astype("int32")
    if "hour" in df.columns:
        df["hour"] = df["hour"].astype("int8")
    if "is_weekend" in df.columns:
        df["is_weekend"] = df["is_weekend"].astype("bool")
    if "is_peak_hour" in df.columns:
        df["is_peak_hour"] = df["is_peak_hour"].astype("bool")

    # Save transformed data
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_file, index=False, compression="snappy")

    logger.info(f"Transformed data saved to {output_file}")
    logger.info(f"Final shape: {df.shape}")
    logger.info(f"Columns: {list(df.columns)}")

    return {
        "input_file": str(input_file),
        "output_file": str(output_file),
        "rows": len(df),
        "columns": list(df.columns),
    }
