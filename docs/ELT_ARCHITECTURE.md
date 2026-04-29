# 🏗️ ELT Pipeline Architecture Documentation

## Overview

**ELT (Extract-Load-Transform) Architecture** untuk NYC TLC + Weather data pipeline.

Perbedaan utama dari ETL yang sebelumnya:
- **ETL**: Extract → Transform (Python) → Load (Parquet files)
- **ELT**: Extract → Load (Database) → Transform (SQL queries in DB)

Untuk TLC data, menggunakan **proper ELT** dengan DuckDB sebagai staging database.

---

## 📊 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                     DATA PIPELINE ARCHITECTURE                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  WEATHER DATA                                                       │
│  ├─ Extract: Open-Meteo API                                         │
│  ├─ Load: Parquet (raw_hourly, raw_daily)                          │
│  └─ Transform: Python (clean, features) → Intermediate Parquet ✅  │
│                                                                     │
│  TLC DATA                                                           │
│  ├─ Extract: NYC TLC parquet files                                 │
│  ├─ Load: DuckDB staging table (tlc_raw)                           │
│  ├─ Transform: SQL queries in DuckDB                               │
│  │  ├─ Filter anomalies (fare, duration, passengers, GPS)          │
│  │  ├─ Cast types (datetime, float, int)                           │
│  │  ├─ Add features (trip_duration_min, tip_pct, hour, day_of_week│
│  │  └─ Result → tlc_cleaned table ✅                              │
│  │                                                                 │
│  └─ Load Final: Star Schema in DuckDB                             │
│     ├─ dim_time (temporal dimensions)                             │
│     ├─ dim_location (pickup/dropoff locations)                    │
│     ├─ dim_weather (weather conditions)                           │
│     └─ fact_trips (central fact table) ✅                         │
│                                                                     │
│  📁 Output: data/final/tlc.duckdb                                  │
│     ├─ tlc_raw (staging)                                          │
│     ├─ tlc_cleaned (intermediate)                                 │
│     ├─ dim_time, dim_location, dim_weather (dimensions)           │
│     └─ fact_trips (fact table)                                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Complete Workflow

### Step 1️⃣: Weather Ingestion + ETL (Python)

**File**: `ingestion/fetch_weather.py`
- Fetch dari Open-Meteo API (Jan-Jun 2025)
- Save hourly + daily aggregated parquet
- Output: `data/raw/weather/weather_*.parquet`

```bash
from ingestion.fetch_weather import ingest_weather_flow
result = ingest_weather_flow()
```

### Step 2️⃣: Weather Preprocessing (ETL in Python)

**File**: `preprocessing/preprocessing_flow.py::preprocessing_weather_flow()`
- Clean anomalies
- Transform + add temporal features
- Output: `data/intermediate/weather/weather_*_transformed.parquet`

### Step 3️⃣: TLC ELT (Load → Transform in DuckDB)

**Files**:
- `preprocessing/clean_tlc.py::load_tlc_to_duckdb()` - Load raw parquet ke DuckDB
- `preprocessing/transform_tlc.py::transform_tlc_in_duckdb()` - SQL transform

**Database**: `data/final/tlc.duckdb`

**Stages**:
1. **EXTRACT**: Raw TLC parquet files dari `ingest_nyc.py`
2. **LOAD**: `load_tlc_to_duckdb()` → Create `tlc_raw` table
3. **TRANSFORM**: `transform_tlc_in_duckdb()` → Create `tlc_cleaned` table
   - Filter anomalies:
     - `fare_amount >= 0`
     - `trip_duration_min between 0 and 300`
     - `passenger_count > 0`
     - Valid GPS coordinates (NYC bounds)
   - Type casting: datetime, float, integer
   - Add features: trip_duration_min, tip_percentage, hour_of_day, day_of_week, is_weekend, is_peak_hour

```python
from preprocessing.preprocessing_flow import preprocessing_tlc_flow

raw_tlc_files = [
    "data/raw/yellow_tripdata_2025-01.parquet",
    "data/raw/yellow_tripdata_2025-02.parquet",
    # ... more months
]

result = preprocessing_tlc_flow(
    raw_tlc_files=raw_tlc_files,
    db_path="data/final/tlc.duckdb"
)
```

### Step 4️⃣: Star Schema Creation (SQL in DuckDB)

**File**: `pipeline/load_star_schema.py`

Creates 4 tables in DuckDB:
1. **dim_time**: Temporal dimensions (date, month, quarter, day_of_week, season, etc.)
2. **dim_location**: Location dimensions (pickup/dropoff coordinates)
3. **dim_weather**: Weather conditions (temperature, humidity, precipitation, weather_code, descriptions)
4. **fact_trips**: Central fact table dengan ALL metrics + foreign keys ke dimensions

**Models** (SQL): `models/dim_*.sql`, `models/fact_trips.sql`

```python
from pipeline.load_star_schema import run_star_schema_pipeline

result = run_star_schema_pipeline(
    db_path="data/final/tlc.duckdb",
    weather_parquet_dir="data/intermediate/weather",
    models_dir="models"
)
```

---

## 🚀 Master Pipeline

**File**: `pipeline/prefect_flow.py::main_pipeline()`

Orchestrates semua stages dengan Prefect:

```python
from pipeline.prefect_flow import main_pipeline

# Run dengan defaults (Jan-Jun 2025)
result = main_pipeline(
    start_date="2025-01-01",
    end_date="2025-06-30",
    db_path="data/final/tlc.duckdb",
    raw_tlc_files=[
        "data/raw/yellow_tripdata_2025-01.parquet",
        "data/raw/yellow_tripdata_2025-02.parquet",
        "data/raw/yellow_tripdata_2025-03.parquet",
        "data/raw/yellow_tripdata_2025-04.parquet",
        "data/raw/yellow_tripdata_2025-05.parquet",
        "data/raw/yellow_tripdata_2025-06.parquet",
    ]
)
```

**Stages** (Sequential):
1. ✅ [STAGE 1] Weather Ingestion (API)
2. ✅ [STAGE 2A] Weather Preprocessing (ETL)
3. ✅ [STAGE 2B] TLC ELT (Load → Transform in DuckDB)
4. ✅ [STAGE 3] Ready untuk next stages

---

## 📊 Key Features

### Anomaly Detection & Filtering (SQL)

Implemented in `transform_tlc_in_duckdb()` dengan WHERE clauses:

| Anomaly Type | Filter | Count |
|---|---|---|
| Negative fare | `fare_amount >= 0` | Logged |
| Invalid duration | `trip_duration BETWEEN 0 AND 300` | Logged |
| Zero passengers | `passenger_count > 0` | Logged |
| Invalid GPS | Coordinates within NYC bounds | Logged |
| **Total removed** | **All above** | **Documented** |

**Output**: Anomaly report dict dengan before/after counts per type

```python
transform_result = {
    "rows_before": 100000,
    "rows_after": 95000,
    "rows_removed": 5000,
    "retention_rate": 95.0,
    "anomalies": {
        "fare_negative": 150,
        "duration_too_long": 2800,
        "passenger_zero": 1200,
        "invalid_gps": 850,
    }
}
```

### Feature Engineering (SQL + Python)

**In Python** (weather_transform.py):
- Temporal: `hour`, `day_of_week`, `is_weekend`, `is_peak_hour`
- Weather: Aggregations (mean, min, max, sum)

**In SQL** (transform_tlc_in_duckdb):
- `trip_duration_min` = (dropoff - pickup) in minutes
- `tip_percentage` = (tip_amount / fare_amount) * 100
- `hour_of_day` = EXTRACT(HOUR FROM pickup_datetime)
- `day_of_week` = DAYNAME(pickup_datetime)
- `is_weekend` = day_of_week IN (Saturday, Sunday)
- `is_peak_hour` = hour_of_day IN (7,8,9,17,18,19)

### Star Schema Design

```sql
-- Relationships
fact_trips.time_key → dim_time.time_key
fact_trips.pickup_location_key → dim_location.location_key
fact_trips.dropoff_location_key → dim_location.location_key
fact_trips.weather_key → dim_weather.weather_key
```

**fact_trips columns** (85+ columns):
- Dimension keys (FK)
- Time details
- Trip metrics (distance, duration, passenger_count)
- Financial metrics (fare, tip, total_amount, tip_percentage)
- Location (coordinates)
- Weather attributes
- Categorization (trip_duration_category, tip_category, temperature_category)

---

## 📁 File Structure

```
RDV-Taxi/
├── preprocessing/
│   ├── clean_tlc.py ......................... Load raw TLC → DuckDB (ELT Extract+Load)
│   ├── transform_tlc.py ..................... SQL transform in DuckDB (ELT Transform)
│   └── preprocessing_flow.py ................ Orchestration tasks + flows
│
├── pipeline/
│   ├── prefect_flow.py ...................... Master pipeline (weather + TLC)
│   └── load_star_schema.py .................. Star schema creation
│
├── models/
│   ├── dim_time.sql ......................... Temporal dimensions
│   ├── dim_location.sql ..................... Location dimensions
│   ├── dim_weather.sql ...................... Weather dimensions
│   └── fact_trips.sql ....................... Central fact table
│
├── data/
│   ├── raw/
│   │   ├── yellow_tripdata_*.parquet ....... TLC raw (from ingest_nyc.py)
│   │   └── weather/ ......................... Weather raw (from fetch_weather.py)
│   ├── intermediate/
│   │   └── weather/ ......................... Weather transformed (parquet)
│   └── final/
│       └── tlc.duckdb ....................... Final DuckDB database (ELT output)
```

---

## 🎯 Usage Examples

### Example 1: Full Pipeline (Weather + TLC)

```python
from pipeline.prefect_flow import main_pipeline

result = main_pipeline(
    start_date="2025-01-01",
    end_date="2025-06-30",
    db_path="data/final/tlc.duckdb",
    raw_tlc_files=[
        "data/raw/yellow_tripdata_2025-01.parquet",
        # ... months 2-6
    ]
)

# Access results
weather_ingestion = result["weather_ingestion"]
tlc_elt = result["tlc_preprocessing"]
print(f"TLC records loaded: {tlc_elt['load']['rows']:,}")
print(f"TLC records after cleaning: {tlc_elt['transform']['rows_after']:,}")
print(f"Retention rate: {tlc_elt['transform']['retention_rate']:.2f}%")
```

### Example 2: TLC ELT Only

```python
from preprocessing.preprocessing_flow import preprocessing_tlc_flow

result = preprocessing_tlc_flow(
    raw_tlc_files=["data/raw/yellow_tripdata_2025-01.parquet"],
    db_path="data/final/tlc.duckdb"
)
```

### Example 3: Create Star Schema After ELT

```python
from pipeline.load_star_schema import run_star_schema_pipeline

result = run_star_schema_pipeline(
    db_path="data/final/tlc.duckdb",
    weather_parquet_dir="data/intermediate/weather",
    models_dir="models"
)

# Verify tables created
summary = result["summary"]
print(f"Total trips: {summary['total_trips']:,}")
print(f"Date range: {summary['date_range']}")
```

### Example 4: Query Star Schema with DuckDB

```python
import duckdb

conn = duckdb.connect("data/final/tlc.duckdb")

# Query example
result = conn.execute("""
    SELECT 
        dt.day_of_week_name,
        dw.temperature_category,
        COUNT(*) as trip_count,
        AVG(ft.total_amount) as avg_fare,
        AVG(ft.tip_percentage) as avg_tip_pct
    FROM fact_trips ft
    JOIN dim_time dt ON ft.time_key = dt.time_key
    JOIN dim_weather dw ON ft.weather_key = dw.weather_key
    GROUP BY 1, 2
    ORDER BY 1, 2
""").fetch_df()

print(result)
```

---

## 🔧 Scheduling with Prefect (Otomasi)

Deploy dengan scheduling:

```bash
# Deploy weather+TLC pipeline dengan daily schedule (2 AM)
prefect deployment build pipeline/prefect_flow.py:main_pipeline \
  --name "tlc-weather-elt" \
  --cron "0 2 * * *" \
  --apply

# Start Prefect worker
prefect worker start
```

---

## ✅ Data Quality Checks

### Before TLC Transform
- Total rows in tlc_raw
- Column data types
- Null counts per column

### After TLC Transform
- Rows removed per anomaly type
- Retention rate
- Column statistics (min, max, avg)

### After Star Schema
- Total trips in fact_trips
- Dimension table row counts
- Foreign key coverage (% of trips with valid FK)
- Financial metrics (revenue, avg fare)

---

## 📝 Next Steps

1. **Run Master Pipeline**: Execute `main_pipeline()` dengan TLC files
2. **Create Star Schema**: Run `load_star_schema.py` untuk SQL transformations
3. **SQL Analysis**: Query fact_trips untuk business questions
4. **ML Modeling**: Use intermediate data untuk prediction models
5. **Dashboard**: Streamlit/Plotly visualization dari star schema
