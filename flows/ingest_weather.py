"""
Prefect flow for fetching Open-Meteo Historical Weather API data.

Fetches historical weather data for NYC and stores it in the raw data directory.
Uses Open-Meteo Archive API (free, no authentication required).

Documentation: https://open-meteo.com/en/docs/historical-weather-api
"""

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from prefect import flow, task
from prefect.utilities.asyncutils import sync_compatible


# Constants
NYC_LATITUDE = 40.7128
NYC_LONGITUDE = -74.0060
OPEN_METEO_API_URL = "https://archive-api.open-meteo.com/v1/archive"

# Weather variables to fetch
WEATHER_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
    "wind_direction_10m",
]


@task(retries=3, retry_delay_seconds=60)
def fetch_weather_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch historical weather data from Open-Meteo Archive API for NYC.
    
    Args:
        start_date: Start date in format 'YYYY-MM-DD'
        end_date: End date in format 'YYYY-MM-DD'
    
    Returns:
        DataFrame with weather data
    
    Raises:
        requests.exceptions.RequestException: If API request fails
    """
    logger = sync_compatible(prefect.get_run_logger)()
    
    logger.info(
        f"Fetching weather data for NYC ({NYC_LATITUDE}, {NYC_LONGITUDE}) "
        f"from {start_date} to {end_date}"
    )
    
    params = {
        "latitude": NYC_LATITUDE,
        "longitude": NYC_LONGITUDE,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(WEATHER_VARIABLES),
        "temperature_unit": "fahrenheit",  # US uses Fahrenheit
        "wind_speed_unit": "mph",  # US uses miles per hour
        "precipitation_unit": "inch",  # US uses inches
        "timezone": "America/New_York",
    }
    
    try:
        response = requests.get(OPEN_METEO_API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        logger.info(f"API Response status: {response.status_code}")
        
        # Parse the hourly data
        hourly_data = data.get("hourly", {})
        times = hourly_data.get("time", [])
        
        if not times:
            raise ValueError("No weather data returned from API")
        
        # Create DataFrame
        weather_df = pd.DataFrame({"datetime": times})
        weather_df["datetime"] = pd.to_datetime(weather_df["datetime"])
        
        # Add each weather variable
        for var in WEATHER_VARIABLES:
            if var in hourly_data:
                weather_df[var] = hourly_data[var]
        
        logger.info(f"Successfully fetched {len(weather_df)} hourly records")
        
        return weather_df
    
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        raise
    except (KeyError, ValueError) as e:
        logger.error(f"Error parsing API response: {str(e)}")
        raise


@task
def aggregate_to_daily(weather_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate hourly weather data to daily level.
    
    Args:
        weather_df: DataFrame with hourly weather data
    
    Returns:
        DataFrame with daily aggregated weather
    """
    logger = sync_compatible(prefect.get_run_logger)()
    
    logger.info("Aggregating hourly weather data to daily level...")
    
    # Create date column
    weather_df["date"] = weather_df["datetime"].dt.date
    
    # Aggregate to daily
    daily_df = weather_df.groupby("date").agg({
        "temperature_2m": ["mean", "min", "max"],
        "relative_humidity_2m": "mean",
        "precipitation": "sum",
        "wind_speed_10m": "mean",
        "wind_direction_10m": "mean",
    }).reset_index()
    
    # Flatten column names
    daily_df.columns = [
        f"{col[0]}_{col[1]}" if col[1] else col[0]
        for col in daily_df.columns.values
    ]
    daily_df.rename(columns={"date": "date"}, inplace=True)
    
    logger.info(f"Aggregated to {len(daily_df)} daily records")
    
    return daily_df


@task
def save_weather_data(weather_df: pd.DataFrame, file_path: str) -> str:
    """
    Save weather data to parquet file.
    
    Args:
        weather_df: DataFrame to save
        file_path: Output file path
    
    Returns:
        Path to saved file
    """
    logger = sync_compatible(prefect.get_run_logger)()
    
    # Create directory if it doesn't exist
    output_dir = Path(file_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)
    
    weather_df.to_parquet(file_path, index=False, compression="snappy")
    
    logger.info(f"Weather data saved to {file_path}")
    logger.info(f"File size: {Path(file_path).stat().st_size / 1024 / 1024:.2f} MB")
    
    return file_path


@flow
def ingest_weather_flow(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_dir: Optional[str] = None,
    days_back: int = 7,
) -> dict:
    """
    Master flow for weather data ingestion from Open-Meteo Archive API.
    
    Fetches historical weather data aligned with NYC TLC dataset date range.
    
    Args:
        start_date: Start date (YYYY-MM-DD). Default: computed from days_back
        end_date: End date (YYYY-MM-DD). Default: today
        output_dir: Output directory path. Default: ./data/raw/weather/
        days_back: Days to look back if start_date not provided. Default: 7 (for testing)
    
    Returns:
        Dictionary with paths to hourly and daily weather data files
    
    Example:
        # Test with 7 days (default)
        ingest_weather_flow()
        
        # Production: Jan-Jun 2025
        ingest_weather_flow(
            start_date="2025-01-01",
            end_date="2025-06-30",
        )
    """
    logger = sync_compatible(prefect.get_run_logger)()
    
    # Set defaults
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    
    if output_dir is None:
        output_dir = os.getenv("RAW_DATA_PATH", "./data/raw") + "/weather"
    
    logger.info(f"Starting weather ingestion flow")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Output directory: {output_dir}")
    
    try:
        # Fetch hourly data
        hourly_data = fetch_weather_data(start_date, end_date)
        
        # Aggregate to daily
        daily_data = aggregate_to_daily(hourly_data)
        
        # Save both hourly and daily
        hourly_file = os.path.join(output_dir, f"weather_hourly_{start_date}_to_{end_date}.parquet")
        daily_file = os.path.join(output_dir, f"weather_daily_{start_date}_to_{end_date}.parquet")
        
        hourly_path = save_weather_data(hourly_data, hourly_file)
        daily_path = save_weather_data(daily_data, daily_file)
        
        logger.info("✓ Weather ingestion flow completed successfully!")
        
        return {
            "hourly_file": hourly_path,
            "daily_file": daily_path,
            "date_range": f"{start_date} to {end_date}",
            "records_hourly": len(hourly_data),
            "records_daily": len(daily_data),
        }
    
    except Exception as e:
        logger.error(f"[ERROR] Weather ingestion failed: {str(e)}")
        raise


if __name__ == "__main__":
    import sys
    
    # Default: 7 days (for testing) - FAST
    if len(sys.argv) == 1:
        print(">>> Running weather ingestion with DEFAULT (7 days) - FAST\n")
        result = ingest_weather_flow()
    
    # Custom: pass days_back or full date range
    # Example: python ingest_weather.py --days=30
    # Example: python ingest_weather.py --start=2025-01-01 --end=2025-06-30
    else:
        # For now, just use defaults
        result = ingest_weather_flow()
    
    print("\n[SUCCESS] Ingestion Complete!")
    print(f"Hourly data: {result['hourly_file']}")
    print(f"Daily data: {result['daily_file']}")
    print(f"Date range: {result['date_range']}")
    print(f"Records (hourly): {result['records_hourly']}")
    print(f"Records (daily): {result['records_daily']}")
