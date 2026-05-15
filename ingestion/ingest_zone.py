"""
Prefect flow for ingesting NYC TLC Taxi Zone Lookup Table and Shapefile.

Downloads the official TLC zone lookup CSV (LocationID → Borough/Zone/service_zone)
AND the official TLC taxi zone shapefile ZIP.
Uses pyshp and pyproj to compute centroid lat/lon per zone (converting from EPSG:2263 to WGS84).
Merges both into an enriched taxi_zone_lookup.csv with full coordinates.
"""

import csv
import logging
import os
import urllib.request
import zipfile
from pathlib import Path

import prefect
from prefect import flow, task
import shapefile
from pyproj import Transformer

logger = logging.getLogger(__name__)

# ─── Source URLs ────────────────────────────────────────────────────────────
# Official TLC lookup CSV (LocationID, Borough, Zone, service_zone)
LOOKUP_CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
# Official TLC taxi zone shapefile ZIP
SHAPEFILE_ZIP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"


# ─── Prefect Tasks ───────────────────────────────────────────────────────────

@task(retries=2, retry_delay_seconds=15)
def download_zone_lookup_csv(output_dir: str) -> str:
    """Download official TLC taxi_zone_lookup.csv."""
    pf_logger = prefect.get_run_logger()
    raw_path = os.path.join(output_dir, "taxi_zone_lookup_raw.csv")

    pf_logger.info(f"Downloading TLC zone lookup CSV → {raw_path}")
    urllib.request.urlretrieve(LOOKUP_CSV_URL, raw_path)
    pf_logger.info("  ✓ Zone lookup CSV downloaded")
    return raw_path


@task(retries=2, retry_delay_seconds=15)
def download_zone_shapefile(output_dir: str) -> str:
    """Download NYC taxi zone Shapefile ZIP."""
    pf_logger = prefect.get_run_logger()
    zip_path = os.path.join(output_dir, "taxi_zones.zip")

    pf_logger.info(f"Downloading NYC taxi zone shapefile ZIP → {zip_path}")
    urllib.request.urlretrieve(SHAPEFILE_ZIP_URL, zip_path)
    pf_logger.info("  ✓ Shapefile ZIP downloaded")
    return zip_path


@task
def process_zones_and_merge(
    lookup_csv_path: str,
    zip_path: str,
    output_dir: str,
) -> str:
    """Extract ZIP, compute centroids from shapefile, and merge with lookup CSV."""
    pf_logger = prefect.get_run_logger()
    
    # ── 1. Extract ZIP ──────────────────────────────────────────────────────
    extract_dir = os.path.join(output_dir, "temp_zones")
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    
    # Find the .shp file (recursive search since ZIP might have subfolders)
    shp_file = next(Path(extract_dir).rglob("*.shp"), None)
    if not shp_file:
        raise FileNotFoundError(f"No .shp file found in {extract_dir}")

    # ── 2. Parse Shapefile & compute centroids ─────────────────────────────
    # NYC State Plane (EPSG:2263) to WGS84 (EPSG:4326)
    transformer = Transformer.from_crs("epsg:2263", "epsg:4326", always_xy=True)
    
    centroids = {}
    with shapefile.Reader(str(shp_file)) as sf:
        for shape_rec in sf.shapeRecords():
            loc_id = shape_rec.record.LocationID
            # Get points from the shape
            points = shape_rec.shape.points
            if not points:
                continue
            
            # Simple centroid: average of all points
            avg_x = sum(p[0] for p in points) / len(points)
            avg_y = sum(p[1] for p in points) / len(points)
            
            # Convert to lat/lon
            lon, lat = transformer.transform(avg_x, avg_y)
            centroids[loc_id] = (round(lat, 6), round(lon, 6))

    pf_logger.info(f"  Computed centroids for {len(centroids)} zones from shapefile")

    # ── 3. Parse lookup CSV and Merge ──────────────────────────────────────
    zones = []
    with open(lookup_csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            loc_id_str = row.get("LocationID", "").strip()
            if not loc_id_str.isdigit():
                continue
            loc_id = int(loc_id_str)
            
            lat, lon = centroids.get(loc_id, (None, None))
            
            zones.append({
                "location_id": loc_id,
                "borough": row.get("Borough", "").strip(),
                "zone": row.get("Zone", "").strip(),
                "service_zone": row.get("service_zone", "").strip(),
                "latitude": lat,
                "longitude": lon
            })

    # ── 4. Write enriched CSV ────────────────────────────────────────────────
    out_path = os.path.join(output_dir, "taxi_zone_lookup.csv")
    fieldnames = ["location_id", "borough", "zone", "service_zone", "latitude", "longitude"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for zone in sorted(zones, key=lambda z: z["location_id"]):
            writer.writerow(zone)

    pf_logger.info(f"  ✓ Enriched zone lookup saved → {out_path}")

    # ── 5. Clean up ─────────────────────────────────────────────────────────
    import shutil
    shutil.rmtree(extract_dir)
    # Path(lookup_csv_path).unlink(missing_ok=True)
    # Path(zip_path).unlink(missing_ok=True)

    return out_path


# ─── Prefect Flow ────────────────────────────────────────────────────────────

@flow(name="Ingest NYC Taxi Zone Lookup")
def ingest_zone_lookup_flow() -> str:
    """Download official TLC data, compute centroids, and save enriched CSV."""
    pf_logger = prefect.get_run_logger()
    pf_logger.info("Starting official Taxi Zone Lookup ingestion...")

    output_dir = os.path.join("data", "raw", "tlc")
    os.makedirs(output_dir, exist_ok=True)

    # Stage 1 – download sources
    lookup_csv = download_zone_lookup_csv(output_dir)
    zip_path = download_zone_shapefile(output_dir)

    # Stage 2 – process & merge
    result_path = process_zones_and_merge(lookup_csv, zip_path, output_dir)

    pf_logger.info(f"✓ Taxi Zone ingestion complete → {result_path}")
    return result_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    ingest_zone_lookup_flow()
