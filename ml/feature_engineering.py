"""Build ML feature matrix from DuckDB data for tip and duration models.

This module reads `data/final/tlc.duckdb`, pulls `tlc_cleaned` and optionally
`weather_transformed`, then writes a modeling dataset to
`data/intermediate/ml_features.parquet`.
"""

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


def _detect_table(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return (
        connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()[0]
        > 0
    )


def build_feature_matrix(
    db_path: str = "data/final/tlc.duckdb",
    output_path: str = "data/intermediate/ml_features.parquet",
) -> pd.DataFrame:
    """Build the ML feature matrix and persist it as parquet."""
    db_file = Path(db_path)
    if not db_file.exists():
        raise FileNotFoundError(f"DuckDB file not found: {db_path}")

    connection = duckdb.connect(database=str(db_file), read_only=True)
    try:
        if not _detect_table(connection, "tlc_cleaned"):
            raise RuntimeError(
                "Table 'tlc_cleaned' not found in DuckDB. Run preprocessing first."
            )

        has_weather = _detect_table(connection, "weather_transformed")
        has_zones = False
        zone_table_name = None
        for candidate in ["zones", "taxi_zone_lookup", "zone_lookup", "taxi_zone"]:
            if _detect_table(connection, candidate):
                has_zones = True
                zone_table_name = candidate
                break

        weather_sql = """
            NULL::DOUBLE AS temperature_c,
            NULL::DOUBLE AS precipitation_mm,
            NULL::DOUBLE AS wind_speed,
            NULL::DOUBLE AS weathercode
        """
        weather_join = ""
        if has_weather:
            weather_sql = """
                w.avg_temperature AS temperature_c,
                w.total_precipitation AS precipitation_mm,
                NULL::DOUBLE AS wind_speed,
                NULL::DOUBLE AS weathercode
            """
            weather_join = "LEFT JOIN weather_transformed w ON CAST(t.pickup_datetime AS DATE) = w.date_actual"

        borough_sql = "NULL::VARCHAR AS borough"
        borough_join = ""
        if has_zones and zone_table_name is not None:
            borough_sql = "CAST(z.Borough AS VARCHAR) AS borough"
            borough_join = f"LEFT JOIN {zone_table_name} z ON t.pickup_location_id = z.LocationID"

        query = f"""
            SELECT
                t.pickup_datetime,
                t.trip_duration_min,
                t.tip_percentage,
                COALESCE(t.hour_of_day, EXTRACT(HOUR FROM t.pickup_datetime)) AS hour,
                COALESCE(t.is_weekend, FALSE) AS is_weekend,
                COALESCE(t.is_peak_hour, FALSE) AS is_peak_hour,
                t.trip_distance,
                t.passenger_count,
                {weather_sql},
                {borough_sql}
            FROM tlc_cleaned t
            {weather_join}
            {borough_join}
        """

        df = connection.execute(query).fetchdf()

        numeric_columns = [
            "temperature_c",
            "precipitation_mm",
            "wind_speed",
            "weathercode",
            "trip_distance",
            "passenger_count",
            "hour",
        ]
        for column in numeric_columns:
            if column in df.columns:
                df[column] = pd.to_numeric(df[column], errors="coerce")
                df[column] = df[column].fillna(df[column].median())

        if "borough" in df.columns:
            df["borough"] = df["borough"].fillna("UNKNOWN")
            df["borough_enc"] = pd.factorize(df["borough"])[0]
        else:
            df["borough_enc"] = 0

        df["is_weekend"] = df["is_weekend"].astype(int)
        df["is_peak_hour"] = df["is_peak_hour"].astype(int)

        feature_columns = [
            "temperature_c",
            "precipitation_mm",
            "wind_speed",
            "weathercode",
            "hour",
            "is_weekend",
            "is_peak_hour",
            "trip_distance",
            "passenger_count",
            "borough_enc",
            "tip_percentage",
            "trip_duration_min",
        ]
        feature_columns = [column for column in feature_columns if column in df.columns]
        df = df[feature_columns]

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(output_file, index=False)

        print(
            f"Saved feature matrix to {output_file} "
            f"({len(df)} rows, {len(df.columns)} columns)"
        )
        return df
    finally:
        connection.close()


if __name__ == "__main__":
    build_feature_matrix()
