"""
Prefect flow for ingesting NYC TLC Trip Record data.

Downloads parquet files from the NYC TLC Trip Record dataset
and stores them in the raw data directory.
"""

import os
import urllib.request
import prefect
from prefect import flow, task

@task(retries=2, retry_delay_seconds=15)
def download_parquet_file(url: str, filepath: str):
    """Download a single parquet file from a given URL with retries on failure."""
    logger = prefect.get_run_logger()
    
    # Cek apakah file sudah pernah di-download sebelumnya agar tidak boros kuota
    if os.path.exists(filepath):
        logger.info(f"File sudah ada, melewati proses download: {filepath}")
        return

    logger.info(f"Mulai mendownload: {url}")
    try:
        # Menarik file dari URL dan menyimpannya ke filepath lokal
        urllib.request.urlretrieve(url, filepath)
        logger.info(f"Sukses disimpan di: {filepath}")
    except Exception as e:
        logger.error(f"Gagal mendownload {url}. Error: {e}")
        raise e

@flow(name="Ingest NYC Taxi Data")
def ingest_nyc_flow(target_month: int = None, target_year: int = None) -> list:
    """Master flow for NYC data ingestion.

    If `target_month` and `target_year` provided, only that month's parquet
    will be downloaded and the function returns a list containing that file path.
    Otherwise, it falls back to downloading the full set (Jan-Jun 2025) for
    backwards compatibility.

    Returns:
        List of downloaded (or existing) file paths.
    """
    logger = prefect.get_run_logger()
    logger.info("Memulai NYC ingestion flow...")

    # 1. Siapkan folder penyimpanan secara otomatis
    output_dir = os.path.join("data", "raw", "tlc")
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Direktori output disiapkan pada folder: {output_dir}/")

    vehicle_types = ["yellow"]
    year = 2025
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    downloaded_files = []

    # Determine months to fetch
    if target_month is not None and target_year is not None:
        months = [int(target_month)]
        year = int(target_year)
    else:
        months = [1, 2, 3, 4, 5, 6]

    # Loop and download only requested months
    for v_type in vehicle_types:
        for month in months:
            month_str = f"{month:02d}"
            filename = f"{v_type}_tripdata_{year}-{month_str}.parquet"
            url = f"{base_url}/{filename}"
            filepath = os.path.join(output_dir, filename)

            # Panggil Prefect Task
            download_parquet_file(url, filepath)
            downloaded_files.append(filepath)

    logger.info("Seluruh proses ingestion selesai!")
    return downloaded_files


if __name__ == "__main__":
    # Default behavior: download full set (backwards compatible)
    ingest_nyc_flow()