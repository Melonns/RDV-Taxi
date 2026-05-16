"""
Kelompok Proyek Rekayasa Data:
1. 235150201111036 DARVESH AZIZ MAWLA
2. 235150207111063 ACHMAD ALVIAN PRASETIO
3. 235150207111006 DZAKY REZANDI
4. 235150201111004 WAHYU DWI LAKSANA PUTRI
5. 235150207111065 JONATHAN SALIM
"""

"""Quick test: download + enrich taxi zone lookup CSV, verify lat/lon populated."""

import csv
import json
import logging
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, ".")
from ingestion.ingest_zone import _centroid_from_geometry, LOOKUP_CSV_URL, GEOJSON_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path("data/raw/tlc")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
FINAL_CSV = OUTPUT_DIR / "taxi_zone_lookup.csv"

# Hapus old file yang tidak punya kolom latitude (legacy)
if FINAL_CSV.exists():
    with open(FINAL_CSV) as f:
        header = f.readline()
    if "latitude" not in header:
        logger.info("Old zone CSV found (no lat/lon) - removing and regenerating...")
        FINAL_CSV.unlink()

if FINAL_CSV.exists():
    logger.info(f"Zone lookup already exists: {FINAL_CSV}")
    with open(FINAL_CSV) as f:
        rows = list(csv.DictReader(f))
    logger.info(f"  Total rows: {len(rows)}")
    for r in rows[:5]:
        lid = r["location_id"]
        zone = r["zone"]
        borough = r["borough"]
        lat = r["latitude"]
        lon = r["longitude"]
        logger.info(f"  [{lid}] {zone} ({borough}) lat={lat} lon={lon}")
    sys.exit(0)

# ── Download zone lookup CSV ──────────────────────────────────────────────────
raw_csv = OUTPUT_DIR / "_taxi_zone_lookup_raw.csv"
logger.info(f"Downloading TLC zone lookup CSV from {LOOKUP_CSV_URL}...")
urllib.request.urlretrieve(LOOKUP_CSV_URL, raw_csv)
logger.info("  Done.")

# ── Download GeoJSON ──────────────────────────────────────────────────────────
geojson_path = OUTPUT_DIR / "_taxi_zones.geojson"
logger.info(f"Downloading NYC taxi zone GeoJSON...")
urllib.request.urlretrieve(GEOJSON_URL, geojson_path)
logger.info("  Done.")

# ── Parse CSV ─────────────────────────────────────────────────────────────────
zones: dict = {}
with open(raw_csv, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        loc_id_raw = (row.get("LocationID") or row.get("location_id") or "").strip()
        if not loc_id_raw.isdigit():
            continue
        loc_id = int(loc_id_raw)
        zones[loc_id] = {
            "location_id": loc_id,
            "borough": (row.get("Borough") or "").strip(),
            "zone": (row.get("Zone") or "").strip(),
            "service_zone": (row.get("service_zone") or "").strip(),
            "latitude": None,
            "longitude": None,
        }
logger.info(f"  Parsed {len(zones)} zones from CSV")

# ── Compute centroids ─────────────────────────────────────────────────────────
with open(geojson_path, encoding="utf-8") as f:
    geojson = json.load(f)

matched = 0
for feature in geojson.get("features", []):
    props = feature.get("properties") or {}
    raw_id = (
        props.get("locationid")
        or props.get("LocationID")
        or props.get("LOCATIONID")
    )
    if raw_id is None:
        continue
    try:
        loc_id = int(raw_id)
    except (ValueError, TypeError):
        continue

    lat, lon = _centroid_from_geometry(feature.get("geometry"))
    if loc_id in zones and lat is not None:
        zones[loc_id]["latitude"] = lat
        zones[loc_id]["longitude"] = lon
        matched += 1

logger.info(f"  Computed centroids for {matched}/{len(zones)} zones")

# ── Save enriched CSV ─────────────────────────────────────────────────────────
fieldnames = ["location_id", "borough", "zone", "service_zone", "latitude", "longitude"]
with open(FINAL_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for zone in sorted(zones.values(), key=lambda z: z["location_id"]):
        writer.writerow({k: zone[k] for k in fieldnames})

# Cleanup temp files
raw_csv.unlink(missing_ok=True)
geojson_path.unlink(missing_ok=True)

logger.info(f"Saved enriched zone lookup: {FINAL_CSV}")

# ── Sample output ─────────────────────────────────────────────────────────────
with open(FINAL_CSV) as f:
    rows = list(csv.DictReader(f))

logger.info(f"Total zones: {len(rows)}")
logger.info("Sample (first 5 zones):")
for r in rows[:5]:
    logger.info(
        f"  [{r['location_id']:>3}] {r['zone']:<35} ({r['borough']:<13})"
        f"  lat={r['latitude']}  lon={r['longitude']}"
    )

null_lat_count = sum(1 for r in rows if not r["latitude"])
logger.info(f"\nZones without coordinates: {null_lat_count}/{len(rows)}")


