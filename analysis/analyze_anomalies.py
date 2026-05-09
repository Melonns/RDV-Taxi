"""Data Quality & Anomaly Analysis Script.

Tugas: Menganalisis tabel 'raw' dan membandingkannya dengan tabel 'cleaned' 
untuk mengetahui seberapa banyak 'kotoran' data yang dibuang.
"""

import duckdb
import pandas as pd
from tabulate import tabulate # Opsional, pakai dataframe.to_string() kalau tidak ada

def analyze_tlc_anomalies(db_path: str):
    conn = duckdb.connect(db_path)
    
    print("\n" + "="*60)
    print("DATA QUALITY REPORT: TLC TAXI DATA")
    print("="*60)
    
    # 1. Summary Statistics
    total_raw = conn.execute("SELECT COUNT(*) FROM tlc_raw").fetchone()[0]
    total_clean = conn.execute("SELECT COUNT(*) FROM tlc_cleaned").fetchone()[0]
    removed = total_raw - total_clean
    retention = (total_clean / total_raw * 100) if total_raw > 0 else 0
    
    summary_data = [
        ["Total Raw Records", f"{total_raw:,}"],
        ["Total Cleaned Records", f"{total_clean:,}"],
        ["Total Removed (Anomalies)", f"{removed:,}"],
        ["Data Retention Rate", f"{retention:.2f}%"]
    ]
    print("\n[SUMMARY]")
    print(tabulate(summary_data, headers=["Metric", "Value"], tablefmt="grid"))
    
    # 2. Breakdown of Anomalies
    print("\n[ANOMALY BREAKDOWN]")
    
    anomalies = {
        "Negative Fare (Fare < 0)": "SELECT COUNT(*) FROM tlc_raw WHERE fare_amount < 0",
        "Missing Passenger (NULL)": "SELECT COUNT(*) FROM tlc_raw WHERE passenger_count IS NULL",
        "Zero Passenger (Count = 0)": "SELECT COUNT(*) FROM tlc_raw WHERE passenger_count = 0",
        "Negative Duration": """SELECT COUNT(*) FROM tlc_raw WHERE 
                                (EXTRACT(EPOCH FROM (CAST(tpep_dropoff_datetime AS TIMESTAMP) - 
                                CAST(tpep_pickup_datetime AS TIMESTAMP))) / 60) < 0""",
        "Too Long Duration (> 5hr)": """SELECT COUNT(*) FROM tlc_raw WHERE 
                                (EXTRACT(EPOCH FROM (CAST(tpep_dropoff_datetime AS TIMESTAMP) - 
                                CAST(tpep_pickup_datetime AS TIMESTAMP))) / 60) > 300""",
        "Invalid Location ID": "SELECT COUNT(*) FROM tlc_raw WHERE PULocationID IS NULL OR DOLocationID IS NULL"
    }
    
    breakdown_data = []
    for label, sql in anomalies.items():
        count = conn.execute(sql).fetchone()[0]
        pct = (count / total_raw * 100) if total_raw > 0 else 0
        breakdown_data.append([label, f"{count:,}", f"{pct:.2f}%"])
    
    print(tabulate(breakdown_data, headers=["Anomaly Type", "Count", "% of Raw"], tablefmt="grid"))
    
    print("\n*Note: Satu baris bisa memiliki lebih dari satu jenis anomali.")
    conn.close()

def analyze_weather_anomalies(db_path: str):
    conn = duckdb.connect(db_path)
    
    print("\n" + "="*60)
    print("DATA QUALITY REPORT: WEATHER DATA")
    print("="*60)
    
    total_raw = conn.execute("SELECT COUNT(*) FROM weather_raw").fetchone()[0]
    total_clean = conn.execute("SELECT COUNT(*) FROM weather_transformed").fetchone()[0]
    
    summary_data = [
        ["Total Raw Weather Records", f"{total_raw:,}"],
        ["Total Cleaned Weather Records", f"{total_clean:,}"],
        ["Anomalies Found", f"{total_raw - total_clean:,}"]
    ]
    print(tabulate(summary_data, tablefmt="grid"))
    
    conn.close()

if __name__ == "__main__":
    db_path = "data/final/tlc.duckdb"
    
    try:
        # Install tabulate if not exists for better printing
        import subprocess
        import sys
        try:
            from tabulate import tabulate
        except ImportError:
            print("Installing tabulate for better visualization...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "tabulate"])
            from tabulate import tabulate
            
        analyze_tlc_anomalies(db_path)
        analyze_weather_anomalies(db_path)
    except Exception as e:
        print(f"Error: {e}")
        print("Pastikan kamu sudah menjalankan load_tlc.py dan load_weather.py")
