"""Weather ingestion flow for Open-Meteo historical data."""

import os
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from prefect import flow, get_run_logger, task


OPEN_METEO_API_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_VARIABLES = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
    "wind_direction_10m",
]


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value is not None else default


def _env_path(name: str, default: str) -> str:
    return os.getenv(name, default)


def _weather_category_from_code(code: float) -> str:
    mapping = {
        0: "Clear",
        1: "Mostly Clear",
        2: "Cloudy",
        3: "Overcast",
        51: "Drizzle",
        53: "Drizzle",
        55: "Drizzle",
        61: "Rain",
        63: "Rain",
        65: "Rain",
        66: "Freezing Rain",
        67: "Freezing Rain",
        71: "Snow",
        73: "Snow",
        75: "Snow",
        77: "Snow",
        80: "Rain",
        81: "Rain",
        82: "Rain",
        85: "Snow",
        86: "Snow",
        95: "Thunderstorm",
        96: "Thunderstorm",
        99: "Thunderstorm",
    }
    if pd.isna(code):
        return "Unknown"
    return mapping.get(int(code), "Other")


@task(retries=3, retry_delay_seconds=60)
def fetch_weather_data(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch hourly historical weather data for NYC from Open-Meteo."""
    logger = get_run_logger()
    latitude = _env_float("NYC_LATITUDE", 40.7128)
    longitude = _env_float("NYC_LONGITUDE", -74.0060)

    logger.info(
        "Fetching weather data for NYC (%s, %s) from %s to %s",
        latitude,
        longitude,
        start_date,
        end_date,
    )

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(WEATHER_VARIABLES),
        "temperature_unit": "celsius",
        "wind_speed_unit": "kmh",
        "precipitation_unit": "mm",
        "timezone": "America/New_York",
    }

    response = requests.get(OPEN_METEO_API_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    hourly_data = data.get("hourly", {})
    times = hourly_data.get("time", [])
    if not times:
        raise ValueError("No weather data returned from API")

    weather_df = pd.DataFrame({"datetime": pd.to_datetime(times)})

    for variable in WEATHER_VARIABLES:
        if variable in hourly_data:
            output_name = "weathercode" if variable == "weather_code" else variable
            weather_df[output_name] = hourly_data[variable]

    if "weathercode" in weather_df.columns:
        weather_df["weather_category"] = weather_df["weathercode"].apply(_weather_category_from_code)

    logger.info("Successfully fetched %s hourly records", len(weather_df))
    return weather_df


@task
def aggregate_to_daily(weather_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate hourly weather data to daily level."""
    logger = get_run_logger()
    logger.info("Aggregating hourly weather data to daily level...")

    working_df = weather_df.copy()
    working_df["date"] = working_df["datetime"].dt.date

    aggregations = {
        "temperature_2m": ["mean", "min", "max"],
        "relative_humidity_2m": "mean",
        "precipitation": "sum",
        "wind_speed_10m": "mean",
        "wind_direction_10m": "mean",
        "weathercode": "mean",
    }

    if "weather_category" in working_df.columns:
        daily_category = (
            working_df.groupby("date")["weather_category"]
            .agg(lambda values: values.mode().iat[0] if not values.mode().empty else values.iloc[0])
            .rename("weather_category")
        )
    else:
        daily_category = None

    daily_df = working_df.groupby("date").agg(aggregations).reset_index()
    daily_df.columns = [
        f"{column[0]}_{column[1]}" if isinstance(column, tuple) and column[1] else column[0]
        if isinstance(column, tuple)
        else column
        for column in daily_df.columns
    ]

    if "weathercode_mean" in daily_df.columns:
        daily_df.rename(columns={"weathercode_mean": "weathercode"}, inplace=True)

    if daily_category is not None:
        daily_df = daily_df.merge(daily_category, on="date", how="left")

    logger.info("Aggregated to %s daily records", len(daily_df))
    return daily_df


@task
def save_weather_data(weather_df: pd.DataFrame, file_path: str) -> str:
    """Save weather data to parquet file."""
    logger = get_run_logger()
    output_path = Path(file_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    weather_df.to_parquet(output_path, index=False, compression="snappy")
    logger.info("Weather data saved to %s", output_path)
    return str(output_path)


@flow
def ingest_weather_flow(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> dict:
    """Master flow for weather ingestion from Open-Meteo archive data.

    Defaults to the January to June 2025 window requested by the team.
    """
    logger = get_run_logger()

    if start_date is None:
        start_date = "2025-01-01"
    if end_date is None:
        end_date = "2025-06-30"
    if output_dir is None:
        output_dir = os.path.join(_env_path("RAW_DATA_PATH", "./data/raw"), "weather")

    logger.info("Starting weather ingestion flow")
    logger.info("Date range: %s to %s", start_date, end_date)
    logger.info("Output directory: %s", output_dir)

    hourly_data = fetch_weather_data(start_date, end_date)
    daily_data = aggregate_to_daily(hourly_data)

    hourly_file = os.path.join(output_dir, f"weather_hourly_{start_date}_to_{end_date}.parquet")
    daily_file = os.path.join(output_dir, f"weather_daily_{start_date}_to_{end_date}.parquet")

    hourly_path = save_weather_data(hourly_data, hourly_file)
    daily_path = save_weather_data(daily_data, daily_file)

    result = {
        "hourly_file": hourly_path,
        "daily_file": daily_path,
        "date_range": f"{start_date} to {end_date}",
        "records_hourly": len(hourly_data),
        "records_daily": len(daily_data),
    }

    logger.info("Weather ingestion flow completed successfully")
    return result


if __name__ == "__main__":
    result = ingest_weather_flow()
    print("\n[SUCCESS] Ingestion Complete!")
    print(f"Hourly data: {result['hourly_file']}")
    print(f"Daily data: {result['daily_file']}")
    print(f"Date range: {result['date_range']}")
    print(f"Records (hourly): {result['records_hourly']}")
    print(f"Records (daily): {result['records_daily']}")