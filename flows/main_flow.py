"""
Master orchestrator for the ELT data pipeline.

Coordinates:
1. NYC TLC Trip Record data ingestion
2. Weather data ingestion
3. dbt transformations
4. Data quality checks
"""

import prefect
from prefect import flow


@flow
def main_elt_pipeline():
    """Master ELT pipeline orchestrator."""
    logger = prefect.get_run_logger()
    logger.info("Starting main ELT pipeline...")
    
    # Import flows
    # from flows.ingest_nyc import ingest_nyc_flow
    # from flows.ingest_weather import ingest_weather_flow
    
    # Run ingest flows
    # ingest_nyc_flow()
    # ingest_weather_flow()
    
    logger.info("ELT pipeline completed successfully!")


if __name__ == "__main__":
    main_elt_pipeline()
