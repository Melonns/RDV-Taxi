"""
Prefect flow for ingesting NYC TLC Taxi Zone Lookup Table.

Downloads the CSV file containing Zone IDs, Boroughs, and Zone Names
and stores it in the raw data directory.
"""

import os
import urllib.request
import prefect
from prefect import flow, task

@task(retries=2, retry_delay_seconds=15)
def download_csv_file(url: str, filepath: str):
    """Download the lookup CSV file from a given URL with retries on failure."""
    logger = prefect.get_run_logger()
    
    # Cek apakah file sudah pernah di-download sebelumnya
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

@flow(name="Ingest NYC Taxi Zone Lookup")
def ingest_zone_lookup_flow():
    """Master flow for NYC Zone Lookup Data ingestion."""
    logger = prefect.get_run_logger()
    logger.info("Memulai ingestion flow untuk Taxi Zone Lookup CSV...")

    # 1. Siapkan folder penyimpanan (agar seragam dengan data Parquet)
    output_dir = "data_raw"
    os.makedirs(output_dir, exist_ok=True)
    
    # 2. Definisikan URL sumber dan nama file output
    # Menggunakan URL resmi dari cloudfront TLC NYC
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    filename = "taxi_zone_lookup.csv"
    filepath = os.path.join(output_dir, filename)
    
    # 3. Panggil Prefect Task untuk men-download file
    download_csv_file(url, filepath)
            
    logger.info("Proses ingestion Taxi Zone Lookup selesai!")

if __name__ == "__main__":
    # Menjalankan script secara langsung (manual) untuk testing
    ingest_zone_lookup_flow()