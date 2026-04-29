# 🚀 Simple ELT Pipeline - No Prefect Required!

## Alur Lengkap (Dari A-Z)

Semua tanpa Prefect, cukup run Python scripts.

---

## 📋 Prasyarat

Pastikan sudah punya:
```bash
# Python packages
pip install duckdb pandas requests numpy prefect

# atau dari requirements.txt
pip install -r requirements.txt
```

---

## 🔄 Complete Workflow (5 Steps)

### **Step 1: Download TLC Data** ⏬

```bash
python ingestion/ingest_nyc.py
```

**Output**:
```
data/raw/
  ├── yellow_tripdata_2025-01.parquet  (Jan 2025)
  ├── yellow_tripdata_2025-02.parquet  (Feb 2025)
  ├── ...
  ├── yellow_tripdata_2025-06.parquet  (Jun 2025)
  └── zones.csv / zones.geojson
```

**Cek**:
```bash
ls -lh data/raw/yellow_tripdata_*.parquet
```

---

### **Step 2: Download Weather Data** 🌤️

```bash
python ingestion/ingest_weather.py
```

atau Python:

```python
from ingestion.fetch_weather import ingest_weather_flow

result = ingest_weather_flow(
    start_date="2025-01-01",
    end_date="2025-06-30"
)
print(f"Weather saved: {result['hourly_file']}, {result['daily_file']}")
```

**Output**:
```
data/raw/weather/
  ├── weather_hourly_2025-01-01_2025-06-30.parquet
  └── weather_daily_2025-01-01_2025-06-30.parquet
```

---

### **Step 3: Clean + Transform Weather** 🧹

```python
from preprocessing.preprocessing_flow import preprocessing_weather_flow

result = preprocessing_weather_flow(
    raw_hourly_file="data/raw/weather/weather_hourly_*.parquet",
    raw_daily_file="data/raw/weather/weather_daily_*.parquet",
    output_dir="data/intermediate"
)

print(f"Weather cleaned: {result['daily_transformed']['rows']:,} rows")
```

**Output**:
```
data/intermediate/weather/
  ├── weather_hourly_cleaned.parquet
  ├── weather_hourly_transformed.parquet
  ├── weather_daily_cleaned.parquet
  └── weather_daily_transformed.parquet
```

---

### **Step 4: Run Complete ELT Pipeline** 🔄

**Option A: Via Terminal (Simplest!)**
```bash
python run_elt_pipeline.py
```

**Option B: Dengan Custom Paths**
```bash
python run_elt_pipeline.py \
  --db data/final/tlc.duckdb \
  --tlc-dir data/raw \
  --weather-dir data/intermediate/weather \
  --models-dir models
```

**Option C: Python Script**
```python
from run_elt_pipeline import run_pipeline

success = run_pipeline(
    db_path="data/final/tlc.duckdb",
    tlc_dir="data/raw",
    weather_dir="data/intermediate/weather",
    models_dir="models"
)

print("✅ Done!" if success else "❌ Failed!")
```

**What Happens**:
```
[STAGE 1] Finding source data files
  ✓ Found 6 TLC files
  ✓ Found weather files

[STAGE 2] Loading TLC to DuckDB (Staging)
  ✓ Created tlc_raw table: 100,000,000 rows

[STAGE 3] Transforming TLC (SQL)
  ✓ Created tlc_cleaned table: 94,765,433 rows
  - Removed invalid fares: 150,234
  - Removed invalid durations: 2,800,345
  - Removed zero passengers: 1,234,988
  - Removed invalid GPS: 1,000,000

[STAGE 4] Loading Weather to DuckDB
  ✓ Created weather_transformed: 181 rows

[STAGE 5] Creating Star Schema
  ✓ dim_time: 182 rows
  ✓ dim_location: 1,250 rows
  ✓ dim_weather: 181 rows
  ✓ fact_trips: 94,765,433 rows

✅ PIPELINE COMPLETED!
```

**Output Database**:
```
data/final/tlc.duckdb
  ├── tlc_raw (staging - raw data)
  ├── tlc_cleaned (intermediate - filtered + features)
  ├── dim_time (dimension - temporal)
  ├── dim_location (dimension - locations)
  ├── dim_weather (dimension - weather)
  └── fact_trips (fact - central table)
```

---

### **Step 5: Query Star Schema** 📊

```python
import duckdb

conn = duckdb.connect("data/final/tlc.duckdb")

# Example 1: Trips by day of week
result = conn.execute("""
    SELECT 
        dt.day_of_week_name,
        COUNT(*) as trip_count,
        ROUND(AVG(ft.total_amount), 2) as avg_fare
    FROM fact_trips ft
    JOIN dim_time dt ON ft.time_key = dt.time_key
    GROUP BY dt.day_of_week_name
    ORDER BY dt.day_of_week_number
""").fetch_df()

print(result)

# Example 2: Peak hours vs off-peak
result2 = conn.execute("""
    SELECT 
        CASE WHEN is_peak_hour THEN 'Peak' ELSE 'Off-peak' END,
        COUNT(*) as trips,
        ROUND(AVG(total_amount), 2) as avg_fare
    FROM fact_trips
    GROUP BY 1
""").fetch_df()

print(result2)
```

---

## 📁 Full Directory Structure After Pipeline

```
RDV-Taxi/
├── run_elt_pipeline.py ..................... Main entry point (tinggal jalanin!)
├── QUICK_START.py .......................... Example queries
├── requirements.txt ........................ Dependencies
│
├── ingestion/
│   ├── ingest_nyc.py ....................... Download TLC
│   └── ingest_weather.py ................... Download weather
│
├── preprocessing/
│   ├── clean_tlc.py ........................ Load to DuckDB
│   ├── transform_tlc.py .................... SQL transform
│   └── preprocessing_flow.py ............... Orchestration
│
├── pipeline/
│   ├── prefect_flow.py ..................... (Optional, Prefect-based)
│   └── load_star_schema.py ................. Star schema loader
│
├── models/
│   ├── dim_time.sql ........................ Temporal dimension
│   ├── dim_location.sql .................... Location dimension
│   ├── dim_weather.sql ..................... Weather dimension
│   └── fact_trips.sql ...................... Central fact table
│
├── data/
│   ├── raw/
│   │   ├── yellow_tripdata_2025-01.parquet
│   │   ├── yellow_tripdata_2025-02.parquet
│   │   ├── ... (sampai 06)
│   │   └── weather/
│   │       ├── weather_hourly_*.parquet
│   │       └── weather_daily_*.parquet
│   ├── intermediate/
│   │   └── weather/
│   │       ├── weather_hourly_transformed.parquet
│   │       └── weather_daily_transformed.parquet
│   └── final/
│       └── tlc.duckdb ...................... 🎯 OUTPUT DATABASE
│
└── docs/
    ├── ELT_ARCHITECTURE.md ................. Full documentation
    └── laporan.docx ........................ (Your final report)
```

---

## 🎯 TL;DR - Just Run This!

```bash
# 1. Download data
python ingestion/ingest_nyc.py
python ingestion/ingest_weather.py

# 2. Run pipeline
python run_elt_pipeline.py

# 3. Query results
python QUICK_START.py

# Done! 🎉
```

---

## 🔍 Troubleshooting

### "No TLC files found"
```bash
# Make sure ingest_nyc.py ran successfully
python ingestion/ingest_nyc.py

# Check files
ls -la data/raw/yellow_tripdata_*.parquet
```

### "Database locked" error
```bash
# Close any other connections
# Or remove old database
rm data/final/tlc.duckdb
python run_elt_pipeline.py
```

### Out of memory
```bash
# Process fewer months
# Edit ingest_nyc.py or run_elt_pipeline.py
# to only load Jan-Feb instead of Jan-Jun
```

---

## 📊 Sample Output

After running `run_elt_pipeline.py`, you get:

```
✅ SUCCESS - Trips by Day of Week:

  day_of_week_name    trip_count  avg_fare  avg_tip_pct
0           Monday      15234567    13.45         15.3
1          Tuesday      14892345    13.21         15.1
2        Wednesday      15123456    13.56         15.4
3         Thursday      15456789    13.78         15.6
4           Friday      16234567    14.23         16.2
5         Saturday      14567890    12.98         14.8
6           Sunday      13456789    12.45         14.2
```

---

## 🚀 Next Steps

1. ✅ Run the pipeline
2. Query star schema untuk analysis
3. Use fact_trips untuk ML modeling
4. Build dashboard (Streamlit/Plotly)
5. Write final report (laporan.docx)

Selesai! 🎉
