"""
Kelompok Proyek Rekayasa Data:
1. 235150201111036 DARVESH AZIZ MAWLA
2. 235150207111063 ACHMAD ALVIAN PRASETIO
3. 235150207111006 DZAKY REZANDI
4. 235150201111004 WAHYU DWI LAKSANA PUTRI
5. 235150207111065 JONATHAN SALIM
"""

import duckdb
import os
from pathlib import Path

def check_parquet_files(directory):
    tlc_dir = Path(directory)
    files = list(tlc_dir.glob("*.parquet"))
    
    conn = duckdb.connect(":memory:")
    
    for f in files:
        print(f"Checking {f.name}...")
        try:
            res = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{str(f)}')").fetchone()[0]
            print(f"  [OK] Valid: {res} rows")
        except Exception as e:
            print(f"  [ERROR] CORRUPTED: {e}")
            # print(f"  Recommendation: Delete {f}")

if __name__ == "__main__":
    print("--- Checking TLC Data ---")
    check_parquet_files("data/raw/tlc")
    print("\n--- Checking Weather Data ---")
    check_parquet_files("data/raw/weather")



