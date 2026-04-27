"""
Prefect flow for ingesting NYC TLC Trip Record data.

Downloads parquet files from the NYC TLC Trip Record dataset
and stores them in the raw data directory.
"""

import prefect
from prefect import flow, task


@task
def download_nyc_data():
    """Download NYC TLC Trip Record data."""
    logger = prefect.get_run_logger()
    logger.info("Starting NYC TLC data download...")
    # Implementation here
    pass


@flow
def ingest_nyc_flow():
    """Master flow for NYC data ingestion."""
    logger = prefect.get_run_logger()
    logger.info("Running NYC ingestion flow...")
    download_nyc_data()


if __name__ == "__main__":
    ingest_nyc_flow()
