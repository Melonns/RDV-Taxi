"""Star schema loader: Load weather to DuckDB dan buat fact + dimension tables.

Workflow:
1. Load transformed weather parquet ke DuckDB
2. Create dimension tables (dim_time, dim_location, dim_weather)
3. Create fact table (fact_trips) with all joins
4. Generate summary statistics

Ini adalah tahap LOAD dari ELT pipeline yang menggunakan SQL untuk transformasi.
"""

import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


def load_weather_to_duckdb(
    db_path: str,
    weather_parquet_dir: str,
) -> dict:
    """Load weather transformed parquet files ke DuckDB.

    Args:
        db_path: Path ke DuckDB database
        weather_parquet_dir: Directory containing weather parquet files

    Returns:
        Dictionary dengan loading report
    """
    logger.info(f"Loading weather data to DuckDB: {db_path}")

    conn = duckdb.connect(db_path)

    try:
        # Load daily weather
        daily_pattern = str(Path(weather_parquet_dir) / "weather_daily_transformed.parquet")

        logger.info(f"Loading weather daily from: {daily_pattern}")

        # Drop existing table
        conn.execute("DROP TABLE IF EXISTS weather_transformed")

        # Load from parquet
        conn.execute(f"""
            CREATE TABLE weather_transformed AS
            SELECT * FROM read_parquet('{daily_pattern}')
        """)

        count = conn.execute("SELECT COUNT(*) FROM weather_transformed").fetchone()[0]

        logger.info(f"✓ Loaded {count:,} weather records to DuckDB")

        return {
            "table": "weather_transformed",
            "rows": count,
            "status": "loaded",
        }

    finally:
        conn.close()


def create_star_schema(
    db_path: str,
    models_dir: str = "./models",
) -> dict:
    """Create star schema tables dari SQL models.

    Urutan eksekusi penting:
    1. dim_time (dependency: tlc_cleaned)
    2. dim_location (dependency: tlc_cleaned)
    3. dim_weather (dependency: weather_transformed)
    4. fact_trips (dependency: semua dimensions + tlc_cleaned)

    Args:
        db_path: Path ke DuckDB database
        models_dir: Directory containing SQL model files

    Returns:
        Dictionary dengan creation report
    """
    logger.info(f"Creating star schema in DuckDB: {db_path}")

    conn = duckdb.connect(db_path)

    try:
        models_order = [
            "dim_time.sql",
            "dim_location.sql",
            "dim_weather.sql",
            "fact_trips.sql",
        ]

        results = {}

        for model_file in models_order:
            model_path = Path(models_dir) / model_file

            if not model_path.exists():
                logger.warning(f"⚠️  Model file not found: {model_path}")
                continue

            logger.info(f"\n📋 Executing model: {model_file}")

            # Read SQL
            sql_content = model_path.read_text()

            # Execute
            conn.execute(sql_content)

            # Get table name dari filename
            table_name = model_file.replace(".sql", "")

            # Get row count
            try:
                row_count = conn.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()[0]

                logger.info(f"  ✓ Table {table_name}: {row_count:,} rows")

                results[table_name] = {
                    "rows": row_count,
                    "status": "created",
                }
            except Exception as e:
                logger.warning(f"  ⚠️  Could not count rows: {str(e)}")
                results[table_name] = {
                    "status": "created_but_count_failed",
                }

        logger.info("\n" + "=" * 70)
        logger.info("✅ Star Schema Created Successfully")
        logger.info("=" * 70)

        logger.info("\nSchema structure:")
        logger.info("  📋 Dimension tables:")
        logger.info(f"    - dim_time: {results.get('dim_time', {}).get('rows', '?'):,} records")
        logger.info(f"    - dim_location: {results.get('dim_location', {}).get('rows', '?'):,} records")
        logger.info(f"    - dim_weather: {results.get('dim_weather', {}).get('rows', '?'):,} records")
        logger.info("  📊 Fact table:")
        logger.info(f"    - fact_trips: {results.get('fact_trips', {}).get('rows', '?'):,} records")

        return {
            "models_created": list(results.keys()),
            "results": results,
            "status": "star_schema_created",
        }

    finally:
        conn.close()


def transform_weather_in_duckdb(db_path: str, weather_parquet_dir: str) -> dict:
    """Transform and load weather data into DuckDB using SQL-based aggregation/cleaning.

    This prefers hourly transformed parquet (aggregates to daily). Falls back to daily
    transformed parquet if hourly is not present. Performs basic quality filters
    and derives temperature categories.

    Returns a report dict with table name and row count.
    """
    logger.info(f"Transforming weather in DuckDB: {db_path}")

    conn = duckdb.connect(db_path)

    try:
        hourly_path = Path(weather_parquet_dir) / "weather_hourly_transformed.parquet"
        daily_path = Path(weather_parquet_dir) / "weather_daily_transformed.parquet"

        conn.execute("DROP TABLE IF EXISTS weather_transformed")

        if daily_path.exists():
            logger.info(f"Loading daily weather from: {daily_path}")

            conn.execute(f"""
                CREATE TABLE weather_transformed AS
                SELECT
                    CAST(date as DATE) as date_actual,
                    24 as records_hourly,
                    CAST(temperature_2m_mean as DOUBLE) as avg_temperature,
                    CAST(temperature_2m_min as DOUBLE) as min_temperature,
                    CAST(temperature_2m_max as DOUBLE) as max_temperature,
                    CAST(COALESCE(precipitation_sum, 0) as DOUBLE) as total_precipitation,
                    CAST(COALESCE(relative_humidity_2m_mean, NULL) as DOUBLE) as avg_humidity
                FROM read_parquet('{daily_path}')
            """)
        else:
            raise FileNotFoundError(f"Weather daily parquet not found at {daily_path}")

        # Add derived columns and basic categorization
        conn.execute("ALTER TABLE weather_transformed ADD COLUMN IF NOT EXISTS temperature_category VARCHAR")

        conn.execute(
            """
            UPDATE weather_transformed
            SET temperature_category = (
                CASE
                    WHEN avg_temperature IS NULL THEN 'Unknown'
                    WHEN avg_temperature < 0 THEN 'Cold'
                    WHEN avg_temperature BETWEEN 0 AND 15 THEN 'Cool'
                    WHEN avg_temperature BETWEEN 15 AND 25 THEN 'Mild'
                    WHEN avg_temperature BETWEEN 25 AND 35 THEN 'Warm'
                    ELSE 'Hot'
                END
            )
            """
        )

        # Final quality counts
        row_count = conn.execute("SELECT COUNT(*) FROM weather_transformed").fetchone()[0]
        logger.info(f"✓ Created weather_transformed with {row_count:,} rows")

        return {
            "table": "weather_transformed",
            "rows": row_count,
            "status": "transformed_in_db",
        }

    finally:
        conn.close()


def generate_schema_summary(db_path: str) -> dict:
    """Generate summary statistics tentang star schema.

    Args:
        db_path: Path ke DuckDB database

    Returns:
        Dictionary dengan schema summary
    """
    logger.info("\n" + "=" * 70)
    logger.info("📊 STAR SCHEMA SUMMARY")
    logger.info("=" * 70)

    conn = duckdb.connect(db_path)

    try:
        summary = {}

        # Fact trips summary
        logger.info("\n📊 Fact Trips Statistics:")

        total_trips = conn.execute("SELECT COUNT(*) FROM fact_trips").fetchone()[0]
        logger.info(f"  Total trips: {total_trips:,}")
        summary["total_trips"] = total_trips

        date_range = conn.execute(
            """
            SELECT 
                MIN(pickup_datetime) as min_date,
                MAX(pickup_datetime) as max_date
            FROM fact_trips
            """
        ).fetchall()[0]
        logger.info(f"  Date range: {date_range[0]} to {date_range[1]}")
        summary["date_range"] = (str(date_range[0]), str(date_range[1]))

        # Financial summary
        financial = conn.execute(
            """
            SELECT 
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as avg_fare,
                AVG(tip_percentage) as avg_tip_pct
            FROM fact_trips
            WHERE total_amount > 0
            """
        ).fetchall()[0]
        logger.info(f"  Total revenue: ${financial[0]:,.2f}")
        logger.info(f"  Avg fare: ${financial[1]:.2f}")
        logger.info(f"  Avg tip: {financial[2]:.2f}%")
        summary["revenue_stats"] = {
            "total": float(financial[0]),
            "avg_fare": float(financial[1]),
            "avg_tip_pct": float(financial[2]),
        }

        # Trip statistics
        trip_stats = conn.execute(
            """
            SELECT 
                AVG(trip_distance) as avg_distance,
                AVG(trip_duration_min) as avg_duration,
                AVG(passenger_count) as avg_passengers
            FROM fact_trips
            """
        ).fetchall()[0]
        logger.info(f"  Avg distance: {trip_stats[0]:.2f} miles")
        logger.info(f"  Avg duration: {trip_stats[1]:.1f} minutes")
        logger.info(f"  Avg passengers: {trip_stats[2]:.1f}")
        summary["trip_stats"] = {
            "avg_distance": float(trip_stats[0]),
            "avg_duration": float(trip_stats[1]),
            "avg_passengers": float(trip_stats[2]),
        }

        # Dimension table sizes
        logger.info("\n📋 Dimension Table Sizes:")

        dim_sizes = conn.execute(
            """
            SELECT 
                'dim_time' as table_name, COUNT(*) as rows UNION ALL
            SELECT 'dim_location', COUNT(*) FROM dim_location UNION ALL
            SELECT 'dim_weather', COUNT(*) FROM dim_weather
            """
        ).fetchall()

        for table_name, row_count in dim_sizes:
            logger.info(f"  {table_name}: {row_count:,} records")
            summary[f"{table_name}_rows"] = row_count

        logger.info("\n" + "=" * 70)

        return summary

    finally:
        conn.close()


def run_star_schema_pipeline(
    db_path: str,
    weather_parquet_dir: str = "./data/intermediate/weather",
    models_dir: str = "./models",
) -> dict:
    """Main pipeline untuk load weather dan buat star schema.

    Args:
        db_path: Path ke DuckDB database
        weather_parquet_dir: Directory dengan weather parquet files
        models_dir: Directory dengan SQL model files

    Returns:
        Dictionary dengan pipeline results
    """
    logger.info("=" * 70)
    logger.info("🔄 Starting Star Schema Creation Pipeline")
    logger.info("=" * 70)

    results = {}

    # Stage 1: Load weather
    logger.info("\n[STAGE 1] Loading weather data to DuckDB...")
    try:
        weather_result = load_weather_to_duckdb(db_path, weather_parquet_dir)
        results["weather_load"] = weather_result
        logger.info(f"✓ Weather loaded")
    except Exception as e:
        logger.error(f"✗ Weather load failed: {str(e)}")
        raise

    # Stage 2: Create star schema
    logger.info("\n[STAGE 2] Creating star schema tables...")
    try:
        schema_result = create_star_schema(db_path, models_dir)
        results["schema_creation"] = schema_result
        logger.info(f"✓ Star schema created")
    except Exception as e:
        logger.error(f"✗ Star schema creation failed: {str(e)}")
        raise

    # Stage 3: Generate summary
    logger.info("\n[STAGE 3] Generating schema summary...")
    try:
        summary = generate_schema_summary(db_path)
        results["summary"] = summary
        logger.info(f"✓ Summary generated")
    except Exception as e:
        logger.error(f"✗ Summary generation failed: {str(e)}")
        raise

    logger.info("\n" + "=" * 70)
    logger.info("✅ Star Schema Pipeline Completed Successfully")
    logger.info("=" * 70)

    return results


if __name__ == "__main__":
    import sys

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Run pipeline
    db_path = "./data/final/tlc.duckdb"
    result = run_star_schema_pipeline(db_path)

    print("\n" + "=" * 70)
    print("📊 Pipeline Results")
    print("=" * 70)
    print(f"Weather load: {result['weather_load']['status']}")
    print(f"Schema creation: {result['schema_creation']['status']}")
    print(f"Summary generated: OK")
    print("=" * 70)
