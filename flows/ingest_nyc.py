"""Legacy compatibility wrapper for NYC TLC ingestion."""

from ingestion.download_tlc import download_nyc_data, ingest_nyc_flow


__all__ = ["download_nyc_data", "ingest_nyc_flow"]


if __name__ == "__main__":
    ingest_nyc_flow()
