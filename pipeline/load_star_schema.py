"""
Kelompok Proyek Rekayasa Data:
1. 235150201111036 DARVESH AZIZ MAWLA
2. 235150207111063 ACHMAD ALVIAN PRASETIO
3. 235150207111006 DZAKY REZANDI
4. 235150201111004 WAHYU DWI LAKSANA PUTRI
5. 235150207111065 JONATHAN SALIM
"""

"""Star schema loader: Load weather to DuckDB dan buat fact + dimension tables.

Workflow:
1. Load transformed weather parquet ke DuckDB
2. Create dimension tables (dim_time, dim_location, dim_weather)
3. Create fact table (fact_trips) with all joins
4. Generate summary statistics

Ini adalah tahap LOAD dari ELT pipeline yang menggunakan SQL untuk transformasi.
"""

import logging
import csv
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)

# Default path untuk enriched zone lookup CSV
DEFAULT_ZONE_LOOKUP_CSV = "./data/raw/tlc/taxi_zone_lookup.csv"


def load_zone_lookup_to_duckdb(conn: duckdb.DuckDBPyConnection, zone_csv_path: str = DEFAULT_ZONE_LOOKUP_CSV) -> int:
    """Load enriched taxi_zone_lookup.csv ke DuckDB sebagai reference table.

    Kolom yang dimuat: location_id, borough, zone, service_zone, latitude, longitude
    Table akan di-DROP dan di-CREATE ulang setiap pipeline run (kecil + jarang berubah).

    Args:
        conn: DuckDB connection yang sudah terbuka
        zone_csv_path: Path ke taxi_zone_lookup.csv hasil ingest_zone_lookup_flow()

    Returns:
        Jumlah baris yang dimuat ke DuckDB
    """
    csv_path = Path(zone_csv_path)
    if not csv_path.exists():
        logger.warning(f"⚠️  Zone lookup CSV tidak ditemukan: {zone_csv_path}")
        logger.warning("    dim_location akan tetap dibuat tapi lat/lon akan NULL")
        logger.warning("    Jalankan ingest_zone_lookup_flow() terlebih dahulu untuk mengisi koordinat.")
        # Buat table kosong agar dim_location.sql tidak error saat LEFT JOIN
        conn.execute("""
            DROP TABLE IF EXISTS taxi_zone_lookup;
            CREATE TABLE taxi_zone_lookup (
                location_id   INTEGER,
                borough       VARCHAR,
                zone          VARCHAR,
                service_zone  VARCHAR,
                latitude      FLOAT,
                longitude     FLOAT
            );
        """)
        return 0

    logger.info(f"Loading taxi zone lookup → DuckDB: {zone_csv_path}")
    conn.execute("""
        DROP TABLE IF EXISTS taxi_zone_lookup;
        CREATE TABLE taxi_zone_lookup AS
        SELECT
            CAST(location_id   AS INTEGER) AS location_id,
            borough,
            zone,
            service_zone,
            CAST(latitude      AS FLOAT)   AS latitude,
            CAST(longitude     AS FLOAT)   AS longitude
        FROM read_csv_auto('{csv}', header=True);
    """.replace("{csv}", str(csv_path).replace("\\", "/")))

    row_count = conn.execute("SELECT COUNT(*) FROM taxi_zone_lookup").fetchone()[0]
    null_lat = conn.execute("SELECT COUNT(*) FROM taxi_zone_lookup WHERE latitude IS NULL").fetchone()[0]
    logger.info(f"  ✓ taxi_zone_lookup: {row_count} zones loaded ({null_lat} without coordinates)")
    return row_count


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
        # Ensure weather_transformed exists so dim_weather.sql can run even when
        # weather ingestion has not been executed yet.
        weather_transformed_exists = conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = 'weather_transformed'
            """
        ).fetchone()[0] > 0

        if not weather_transformed_exists:
            logger.warning("⚠️  weather_transformed table not found; creating empty fallback table")
            conn.execute(
                """
                CREATE OR REPLACE TABLE weather_transformed AS
                SELECT
                    CAST(NULL AS DATE) AS date_actual,
                    CAST(NULL AS DOUBLE) AS avg_temperature,
                    CAST(NULL AS DOUBLE) AS avg_humidity,
                    CAST(NULL AS DOUBLE) AS total_precipitation,
                    CAST(NULL AS DOUBLE) AS wind_speed,
                    CAST(NULL AS DOUBLE) AS wind_direction,
                    CAST(NULL AS INTEGER) AS weather_code,
                    CAST(NULL AS VARCHAR) AS weather_category,
                    CAST(NULL AS VARCHAR) AS temperature_category
                WHERE FALSE;
                """
            )

        # ── Pre-step: Load taxi zone lookup (needed by dim_location.sql JOIN) ──
        zone_csv = Path(models_dir).parent / "data" / "raw" / "tlc" / "taxi_zone_lookup.csv"
        # Fallback ke default path jika relative path tidak ketemu
        if not zone_csv.exists():
            zone_csv = Path(DEFAULT_ZONE_LOOKUP_CSV)
        load_zone_lookup_to_duckdb(conn, str(zone_csv))

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

    # Stage 1: Skip (already handled by preprocessing)
    results["weather_load"] = {"status": "skipped_already_processed"}

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


