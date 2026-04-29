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
def ingest_nyc_flow():
    """Master flow for NYC data ingestion."""
    logger = prefect.get_run_logger()
    logger.info("Memulai NYC ingestion flow...")

    # 1. Siapkan folder penyimpanan secara otomatis
    output_dir = "data/raw/tlc"
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Direktori output disiapkan pada folder: {output_dir}/")

    # 2. Definisikan parameter data sesuai batasan proyek
    vehicle_types = ["yellow"]
    year = 2025
    months = [1, 2, 3, 4, 5, 6]  # Januari sampai Juni
    
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    # 3. Looping untuk mengeksekusi penarikan data
    for v_type in vehicle_types:
        for month in months:
            # Format bulan menjadi dua digit (01, 02, 03, 04)
            month_str = f"{month:02d}"
            filename = f"{v_type}_tripdata_{year}-{month_str}.parquet"
            
            url = f"{base_url}/{filename}"
            filepath = os.path.join(output_dir, filename)
            
            # Panggil Prefect Task
            download_parquet_file(url, filepath)
            
    logger.info("Seluruh proses ingestion selesai!")

if __name__ == "__main__":
    # KODE LAMA (Manual):
    ingest_nyc_flow()
    # Membuat jadwal menggunakan format Cron 
    # (Contoh: "0 0 1 * *" artinya jalan otomatis setiap tanggal 1 jam 00:00)
    # jadwal_otomatis = CronSchedule(cron="0 0 1 * *", timezone="Asia/Jakarta")

    # print("Mengaktifkan jadwal otomatisasi ingestion...")
    
    # # Mendaftarkan flow ke Prefect Server agar jalan otomatis
    # ingest_nyc_flow.serve(
    #     name="jadwal-bulanan-nyc-taxi",
    #     schedule=jadwal_otomatis
    # )