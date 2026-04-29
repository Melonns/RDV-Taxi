"""NYC TLC ingestion module aligned with the proposed architecture."""

from prefect import flow, get_run_logger, task


@task
def download_nyc_data() -> None:
    """Placeholder task for downloading NYC TLC data."""
    logger = get_run_logger()
    logger.info("TODO: implement NYC TLC download.")


@flow
def ingest_nyc_flow() -> None:
    """Master flow for NYC TLC ingestion."""
    logger = get_run_logger()
    logger.info("Running NYC TLC ingestion flow...")
    download_nyc_data()


if __name__ == "__main__":
    ingest_nyc_flow()
