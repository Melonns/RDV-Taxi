"""Legacy compatibility wrapper for weather ingestion."""

from ingestion.fetch_weather import fetch_weather_data, ingest_weather_flow


__all__ = ["fetch_weather_data", "ingest_weather_flow"]


if __name__ == "__main__":
    ingest_weather_flow()
    
    print("\n[SUCCESS] Ingestion Complete!")
    print(f"Hourly data: {result['hourly_file']}")
    print(f"Daily data: {result['daily_file']}")
    print(f"Date range: {result['date_range']}")
    print(f"Records (hourly): {result['records_hourly']}")
    print(f"Records (daily): {result['records_daily']}")
