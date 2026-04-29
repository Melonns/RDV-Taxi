"""Weather ingestion module aligned with the proposed architecture."""

from prefect import flow, get_run_logger, task


@task
def fetch_weather_data() -> None:
    """Placeholder task for fetching weather data."""
    logger = get_run_logger()
    logger.info("TODO: implement weather fetch.")


@flow
def ingest_weather_flow() -> None:
    """Master flow for weather ingestion."""
    logger = get_run_logger()
    logger.info("Running weather ingestion flow...")
    fetch_weather_data()


if __name__ == "__main__":
    ingest_weather_flow()
