"""Master Prefect pipeline for the project."""

from prefect import flow, get_run_logger


@flow
def main_pipeline() -> None:
    """Master pipeline orchestrator."""
    logger = get_run_logger()
    logger.info("TODO: wire ingestion, preprocessing, modeling, and dashboard inputs.")


if __name__ == "__main__":
    main_pipeline()
