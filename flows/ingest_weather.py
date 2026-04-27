"""
Prefect flow for fetching Open-Meteo Historical Weather API data.

Fetches weather data for NYC and stores it in the raw data directory.
"""

import prefect
from prefect import flow, task


@task
def fetch_weather_data():
    """Fetch weather data from Open-Meteo API."""
    logger = prefect.get_run_logger()
    logger.info("Starting weather data fetch...")
    # Implementation here
    pass


@flow
def ingest_weather_flow():
    """Master flow for weather data ingestion."""
    logger = prefect.get_run_logger()
    logger.info("Running weather ingestion flow...")
    fetch_weather_data()


if __name__ == "__main__":
    ingest_weather_flow()
