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