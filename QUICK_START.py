"""
Quick Start Guide - ELT Pipeline Tanpa Prefect

Alur lengkap dari A-Z, tinggal jalanin!
"""

# ============================================================================
# STEP 1: Download TLC Data (Run once)
# ============================================================================
# Terminal:
#   python ingestion/ingest_nyc.py
#
# Output:
#   - data/raw/yellow_tripdata_2025-01.parquet
#   - data/raw/yellow_tripdata_2025-02.parquet
#   - ... (sampai bulan 06)
#   - data/raw/zones.csv (atau shapes JSON)
#
# Cek besar file:
#   ls -lh data/raw/yellow_tripdata_*.parquet


# ============================================================================
# STEP 2: Ingest Weather Data (Run once)
# ============================================================================
# Terminal:
#   python ingestion/ingest_weather.py
#   (atau dari kode Python:)
#
# Python:
from ingestion.fetch_weather import ingest_weather_flow

weather_result = ingest_weather_flow(
    start_date="2025-01-01",
    end_date="2025-06-30",
    output_dir="data/raw"
)

print(f"Weather data ingested:")
print(f"  Hourly file: {weather_result['hourly_file']}")
print(f"  Daily file: {weather_result['daily_file']}")
print(f"  Records: {weather_result['records_hourly']:,} hourly, {weather_result['records_daily']:,} daily")


# ============================================================================
# STEP 3: Preprocess Weather (Clean + Transform)
# ============================================================================
# Python:
from preprocessing.preprocessing_flow import preprocessing_weather_flow

weather_preprocessing = preprocessing_weather_flow(
    raw_hourly_file=weather_result["hourly_file"],
    raw_daily_file=weather_result["daily_file"],
    output_dir="data/intermediate"
)

print(f"\nWeather preprocessing done:")
print(f"  Hourly transformed: {weather_preprocessing['hourly_transformed']['rows']:,} rows")
print(f"  Daily transformed: {weather_preprocessing['daily_transformed']['rows']:,} rows")


# ============================================================================
# STEP 4: Run Complete ELT Pipeline (TLC + Weather + Star Schema)
# ============================================================================
# Terminal (simplest):
#   python run_elt_pipeline.py
#
# atau Python:
from run_elt_pipeline import run_pipeline

success = run_pipeline(
    db_path="data/final/tlc.duckdb",
    tlc_dir="data/raw",
    weather_dir="data/intermediate/weather",
    models_dir="models"
)

if success:
    print("\n✅ Pipeline completed!")
else:
    print("\n❌ Pipeline failed!")


# ============================================================================
# STEP 5: Query Results dari DuckDB
# ============================================================================
# Python:
import duckdb

conn = duckdb.connect("data/final/tlc.duckdb")

# List tables
tables = conn.execute("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema='main'
""").fetch_df()

print("\nTables in database:")
print(tables)


# ============================================================================
# EXAMPLE QUERIES
# ============================================================================

# Query 1: Total trips by day of week
result1 = conn.execute("""
    SELECT 
        dt.day_of_week_name,
        COUNT(*) as trip_count,
        ROUND(AVG(ft.total_amount), 2) as avg_fare,
        ROUND(AVG(ft.tip_percentage), 2) as avg_tip_pct
    FROM fact_trips ft
    JOIN dim_time dt ON ft.time_key = dt.time_key
    GROUP BY dt.day_of_week_name
    ORDER BY 
        CASE dt.day_of_week_number 
            WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 3 
            WHEN 4 THEN 4 WHEN 5 THEN 5 WHEN 6 THEN 6 
            WHEN 0 THEN 7 END
""").fetch_df()

print("\n📊 Trips by Day of Week:")
print(result1)


# Query 2: Peak hour analysis
result2 = conn.execute("""
    SELECT 
        CASE WHEN is_peak_hour THEN 'Peak (7-9, 17-19)' ELSE 'Off-peak' END as time_period,
        COUNT(*) as trip_count,
        ROUND(AVG(total_amount), 2) as avg_fare,
        ROUND(AVG(trip_duration_min), 2) as avg_duration_min,
        ROUND(AVG(trip_distance), 2) as avg_distance_miles
    FROM fact_trips
    GROUP BY 1
""").fetch_df()

print("\n⏱️  Peak Hours Analysis:")
print(result2)


# Query 3: Weather impact on trips
result3 = conn.execute("""
    SELECT 
        dw.temperature_category,
        dw.weather_description,
        COUNT(*) as trip_count,
        ROUND(AVG(ft.total_amount), 2) as avg_fare,
        ROUND(AVG(ft.tip_percentage), 2) as avg_tip_pct
    FROM fact_trips ft
    JOIN dim_weather dw ON ft.weather_key = dw.weather_key
    WHERE dw.temperature_category != 'Unknown'
    GROUP BY 1, 2
    ORDER BY trip_count DESC
    LIMIT 10
""").fetch_df()

print("\n🌤️  Weather Impact on Trips:")
print(result3)


# Query 4: Top pickup locations
result4 = conn.execute("""
    SELECT 
        dl.location_id,
        ROUND(dl.latitude, 4) as lat,
        ROUND(dl.longitude, 4) as lon,
        COUNT(*) as pickup_count,
        ROUND(AVG(ft.total_amount), 2) as avg_fare
    FROM fact_trips ft
    JOIN dim_location dl ON ft.pickup_location_key = dl.location_key
    GROUP BY 1, 2, 3
    ORDER BY pickup_count DESC
    LIMIT 10
""").fetch_df()

print("\n📍 Top 10 Pickup Locations:")
print(result4)


# Query 5: Revenue analysis
result5 = conn.execute("""
    SELECT 
        EXTRACT(MONTH FROM ft.pickup_datetime) as month,
        dt.day_of_week_name,
        COUNT(*) as trips,
        ROUND(SUM(ft.total_amount), 2) as total_revenue,
        ROUND(AVG(ft.total_amount), 2) as avg_fare,
        ROUND(SUM(ft.tip_amount), 2) as total_tips
    FROM fact_trips ft
    JOIN dim_time dt ON ft.time_key = dt.time_key
    GROUP BY 1, 2
    ORDER BY 1, CASE dt.day_of_week_number 
        WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 3 
        WHEN 4 THEN 4 WHEN 5 THEN 5 WHEN 6 THEN 6 
        WHEN 0 THEN 7 END
""").fetch_df()

print("\n💰 Revenue by Month and Day of Week:")
print(result5)


# ============================================================================
# EXPORT TO CSV (untuk dashboard atau analisis lanjutan)
# ============================================================================

# Export fact_trips untuk ML modeling
fact_trips_df = conn.execute("SELECT * FROM fact_trips LIMIT 1000000").fetch_df()
fact_trips_df.to_csv("data/analysis/fact_trips_export.csv", index=False)
print("\n💾 Exported: data/analysis/fact_trips_export.csv")

# Export for visualization
daily_stats = conn.execute("""
    SELECT 
        dt.date_actual,
        COUNT(*) as trip_count,
        ROUND(AVG(ft.total_amount), 2) as avg_fare,
        ROUND(SUM(ft.total_amount), 2) as total_revenue,
        ROUND(AVG(ft.trip_distance), 2) as avg_distance
    FROM fact_trips ft
    JOIN dim_time dt ON ft.time_key = dt.time_key
    GROUP BY 1
    ORDER BY 1
""").fetch_df()
daily_stats.to_csv("data/analysis/daily_stats.csv", index=False)
print("💾 Exported: data/analysis/daily_stats.csv")

conn.close()
print("\n✅ All done!")
