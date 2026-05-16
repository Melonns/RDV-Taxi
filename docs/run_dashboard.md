# Panduan Menjalankan Dashboard RDV Taxi

Dokumen ini menjelaskan langkah menjalankan dashboard Streamlit untuk proyek:

**Analisis Pengaruh Kondisi Cuaca terhadap Persentase Tip dan Durasi Perjalanan NYC Taxi**

Dataset utama yang digunakan adalah **NYC TLC Yellow Taxi Trip Records** periode **Januari–Juni 2025**, dengan data cuaca dari **Open-Meteo Historical Weather API**.

---



## 1. Clone Repository

```bash
git clone https://github.com/Melonns/RDV-Taxi.git
cd RDV-Taxi
```

Jika repository sudah ada di lokal, cukup masuk ke folder project:

```bash
cd RDV-Taxi
```

---

## 2. Buat Virtual Environment

### Windows PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\activate
```

Jika PowerShell menolak aktivasi virtual environment, jalankan:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\activate
```

### Mac/Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Jika berhasil, terminal akan menampilkan prefix seperti:

```text
(.venv)
```

---

## 3. Install Dependency

```bash
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

Cek dependency utama dashboard:

```bash
python -c "import duckdb, pandas, streamlit, plotly, pydeck; print('OK')"
```

Jika output menampilkan:

```text
OK
```

maka environment sudah siap.

---

## 5. Struktur Data yang Dibutuhkan

Folder `data/` tidak ikut masuk GitHub karena masuk `.gitignore`.

Dashboard membutuhkan output berikut:

```text
data/
├── raw/
│   ├── tlc/
│   │   ├── yellow_tripdata_2025-01.parquet
│   │   ├── yellow_tripdata_2025-02.parquet
│   │   ├── yellow_tripdata_2025-03.parquet
│   │   ├── yellow_tripdata_2025-04.parquet
│   │   ├── yellow_tripdata_2025-05.parquet
│   │   ├── yellow_tripdata_2025-06.parquet
│   │   └── taxi_zone_lookup.csv
│   └── weather/
│       └── weather_daily_2025-01-01_to_2025-06-30.parquet
├── intermediate/
│   ├── cleaned.parquet
│   ├── ml_features.parquet
│   └── ml_results.parquet
└── final/
    └── tlc.duckdb
```

File paling penting untuk dashboard:

```text
data/final/tlc.duckdb
data/intermediate/ml_results.parquet
```

---

## 5. Jalankan Ingestion Data

### 5.1 Download Data TLC Yellow Taxi Januari–Juni 2025

Jalankan per bulan:

```powershell
python -c "from ingestion.ingest_nyc import ingest_nyc_flow; ingest_nyc_flow(target_month=1, target_year=2025)"
python -c "from ingestion.ingest_nyc import ingest_nyc_flow; ingest_nyc_flow(target_month=2, target_year=2025)"
python -c "from ingestion.ingest_nyc import ingest_nyc_flow; ingest_nyc_flow(target_month=3, target_year=2025)"
python -c "from ingestion.ingest_nyc import ingest_nyc_flow; ingest_nyc_flow(target_month=4, target_year=2025)"
python -c "from ingestion.ingest_nyc import ingest_nyc_flow; ingest_nyc_flow(target_month=5, target_year=2025)"
python -c "from ingestion.ingest_nyc import ingest_nyc_flow; ingest_nyc_flow(target_month=6, target_year=2025)"
```

Cek hasil download:

```powershell
dir data\raw\tlc
```

Minimal harus ada:

```text
yellow_tripdata_2025-01.parquet
yellow_tripdata_2025-02.parquet
yellow_tripdata_2025-03.parquet
yellow_tripdata_2025-04.parquet
yellow_tripdata_2025-05.parquet
yellow_tripdata_2025-06.parquet
```

### 5.2 Download Taxi Zone Lookup

```powershell
python -m ingestion.ingest_zone
```

Cek file:

```powershell
dir data\raw\tlc
```

Pastikan ada:

```text
taxi_zone_lookup.csv
```

### 5.3 Download Data Cuaca

```powershell
python -c "from ingestion.fetch_weather import ingest_weather_flow; ingest_weather_flow()"
```

Cek hasil:

```powershell
dir data\raw\weather
```

Pastikan ada file weather untuk periode Januari–Juni 2025.

---

## 6. Jalankan Pipeline ELT

Setelah raw data tersedia, jalankan:

```powershell
python run_elt_pipeline.py
```

Pipeline akan membuat:

```text
data/final/tlc.duckdb
data/intermediate/cleaned.parquet
data/intermediate/ml_features.parquet
data/intermediate/ml_results.parquet
ml/saved/rf_tip.pkl
ml/saved/rf_duration.pkl
```

Catatan:

- Folder `data/` dan `ml/saved/` tidak ikut dipush ke GitHub.
- `data/final/tlc.duckdb` adalah database utama yang dibaca dashboard.
- `data/intermediate/ml_results.parquet` digunakan untuk tab ML Insights.

---

## 7. Validasi Output Pipeline

Cek apakah database final sudah terbentuk:

```powershell
python -c "import os; print(os.path.exists('data/final/tlc.duckdb'))"
```

Output yang diharapkan:

```text
True
```

Cek tabel dalam DuckDB:

```powershell
python -c "import duckdb; con=duckdb.connect('data/final/tlc.duckdb'); print(con.execute('SHOW TABLES').fetchall()); con.close()"
```

Minimal harus ada tabel:

```text
fact_trips
dim_time
dim_location
dim_weather
tlc_cleaned
weather_transformed
```

Cek hasil ML:

```powershell
python -c "import pandas as pd; df=pd.read_parquet('data/intermediate/ml_results.parquet'); print(df.head()); print(df.columns)"
```

Kolom yang dibutuhkan dashboard:

```text
feature_name
importance_tip
importance_duration
importance_avg
is_weather
```

---

## 8. Jalankan Dashboard

```powershell
streamlit run dashboard/app.py
```

Dashboard akan terbuka otomatis di browser. Jika tidak terbuka, akses manual:

```text
http://localhost:8501
```

---

## 9. Struktur Dashboard

Dashboard terdiri dari 4 tab utama:

### 9.1 Ringkasan

Menampilkan:

- Total perjalanan
- Rata-rata persentase tip
- Rata-rata durasi perjalanan
- Distribusi perjalanan berdasarkan kategori cuaca

### 9.2 Peta

Menampilkan:

- Heatmap zona pickup menggunakan PyDeck
- Volume perjalanan per borough
- Top 25 zona pickup berdasarkan volume perjalanan

Catatan: peta menggunakan koordinat centroid dari `dim_location`, bukan titik GPS asli dari setiap perjalanan, karena data TLC Yellow Taxi 2025 berbasis `LocationID`.

### 9.3 Cuaca vs Tip & Durasi

Menampilkan:

- Rata-rata tip per kategori cuaca
- Rata-rata durasi per kategori cuaca
- Tren tip atau durasi berdasarkan jam
- Perbandingan durasi pada peak hour dan non-peak hour

### 9.4 ML Insights

Menampilkan:

- Feature importance untuk model prediksi tip
- Feature importance untuk model prediksi durasi
- Kontribusi fitur cuaca terhadap masing-masing target

---

## 10. Filter Dashboard

Dashboard menyediakan filter:

- Rentang tanggal
- Kategori cuaca
- Kategori suhu
- Borough
- Payment type, jika kolom tersedia di `fact_trips`

Filter utama yang digunakan dashboard:

```sql
pickup_datetime >= DATE '2025-01-01'
AND pickup_datetime < DATE '2025-07-01'
AND tip_percentage BETWEEN 0 AND 100
```

Filter `tip_percentage` 0–100% digunakan untuk mengurangi pengaruh outlier ekstrem.

---