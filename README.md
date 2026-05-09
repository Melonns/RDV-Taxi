# NYC TLC Trip Record Analysis - ELT Pipeline

An end-to-end ELT (Extract, Load, Transform) data pipeline analyzing NYC TLC trip records with historical weather data correlation.

## 🏗️ Project Architecture

```
ELT Pipeline
├── Extract (Prefect)
│   ├── NYC TLC Trip Records (Parquet files)
│   └── Open-Meteo Weather Data (API)
├── Load (DuckDB/PostgreSQL)
│   └── Raw Data Storage
├── Transform (dbt)
│   └── Data Modeling & Aggregations
└── Visualize (Streamlit)
    └── Interactive Dashboard
```

## 📂 Project Structure

```
.
├── data/                          # Data storage
│   ├── raw/                       # Raw ingested data
│   ├── intermediate/              # Staging area for transformations
│   └── final/                     # Final analysis-ready data
├── ingestion/                     # Data ingestion
│   ├── download_tlc.py            # NYC TLC ingestion
│   └── fetch_weather.py           # Weather API ingestion
├── pipeline/                      # Prefect orchestration
│   └── prefect_flow.py            # Master orchestrator
├── preprocessing/                 # Cleaning and transformation
│   ├── clean.py
│   └── transform.py
├── dbt_project/                   # dbt transformation models
├── models/                        # dbt-style SQL models
├── analysis/                      # SQL analysis queries
├── ml/                            # Feature engineering and ML models
├── schema/                        # Star schema definition
├── dashboard/                     # Streamlit visualization
│   ├── app.py                    # Main dashboard app
│   └── components/               # Reusable UI components
├── docs/                          # Documentation & screenshots
│   └── screenshots/
├── requirements.txt               # Python dependencies
├── .env.example                   # Environment variables template
├── .gitignore                     # Git ignore rules
└── README.md                      # This file
```

## 🚀 Getting Started

### 1. Clone and Setup

```bash
cd RDV-Taxi
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your database and API configurations
```

### 3. Initialize dbt

```bash
cd dbt_project
dbt init  # Or use existing configuration
```

### 4. Start Prefect Server (Optional)

```bash
prefect server start
```

### 5. Run the Pipeline

```bash
# Run main orchestrator
python flows/main_flow.py

# Or run individual flows
python flows/ingest_nyc.py
python flows/ingest_weather.py
```

### 6. View Dashboard

```bash
streamlit run dashboard/app.py
```

## 🛠️ Pipeline — Run & Deployment (for Data Analysts)

This project uses Prefect to orchestrate an incremental monthly batch ELT pipeline. Below are clear steps to run, schedule, and verify the pipeline both for demo/testing and production.

Prerequisites:
- Activate the project virtualenv and install `requirements.txt`.
- Ensure `data/raw/tlc` contains the TLC parquet files (or allow the ingestion flow to download them).

1) Start the Prefect UI (Orion) locally

```bash
# In terminal 1: start Orion (UI)
prefect orion start
# Open http://127.0.0.1:4200 in your browser
```

2) Run the pipeline manually (single batch)

```bash
# Manual single execution — pipeline will process the next unprocessed month
python pipeline/prefect_flow.py
```

3) Demo mode — schedule every 5 minutes (recommended for rapid testing)

```bash
# Register & serve a deployment that runs every 5 minutes
python pipeline/prefect_flow.py --mode demo

# Ensure an agent/worker is running to execute scheduled runs if needed:
prefect agent start
```

4) Production mode — schedule daily at 00:00 (midnight)

```bash
python pipeline/prefect_flow.py --mode production

# Or build and apply a deployment explicitly:
prefect deployment build pipeline/prefect_flow.py:main_pipeline --name taxi-elt-batch-production --cron "0 0 * * *" --apply
prefect agent start
```

Notes on scheduling behavior
- Demo mode creates a deployment with cron `*/5 * * * *` (every 5 minutes) and is useful to simulate sequential monthly ingestion quickly.
- Production mode uses `0 0 * * *` (daily midnight) to process one month per run and mimic slower production cadence.

5) Verify runs and pipeline state

```bash
# Check Prefect UI: http://127.0.0.1:4200/deployments and /flow-runs

# Check pipeline state (which month was last processed):
python -c "from pipeline.batch_state import read_state; import json; print(json.dumps(read_state(), indent=2))"

# Inspect final DuckDB to validate loaded data:
python - <<'PY'
import duckdb
conn = duckdb.connect('data/final/tlc.duckdb')
print(conn.execute('SHOW TABLES').fetchall())
print(conn.execute('SELECT COUNT(*) FROM tlc_raw').fetchall())
print(conn.execute('SELECT MIN(lpep_pickup_datetime), MAX(lpep_pickup_datetime) FROM tlc_raw').fetchall())
conn.close()
PY
```

6) Reset / clean (for testing from a clean slate)

```bash
# Reset pipeline state to start from January 2025
python -c "from pipeline.batch_state import reset_state; reset_state()"

# Remove DuckDB to force fresh load
rm -f data/final/tlc.duckdb

# Optionally clear intermediate files
rm -rf data/intermediate/*
```

Important note:
- The cumulative batch mode now appends monthly TLC data into DuckDB.
- If you previously ran the older overwrite-based pipeline, delete `data/final/tlc.duckdb` once so the new cumulative mode starts from a clean slate.
- After that, each new batch run will keep previous batch data instead of replacing it.

7) Troubleshooting & tips

- If scheduled runs don't execute, ensure an agent is running (`prefect agent start`).
- If the pipeline appears to process all months even after scheduling per-month batches, check `data/pipeline_state.json` — it determines the next month to ingest. Reset it to re-run from start.
- Logs for each flow run are available in the Prefect UI; detailed task logs show whether files were downloaded or skipped because they already exist.

If you want, I can also add a short `run_pipeline.sh` helper script that wraps the steps above (reset, run once, check state). Want me to add that? 

## 📊 Data Sources

- **NYC TLC Trip Records**: Parquet files from NYC TLC website
- **Weather Data**: Open-Meteo Historical Weather API (free, no authentication)

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Orchestration | Prefect |
| Data Warehouse | DuckDB / PostgreSQL |
| Transformation | dbt |
| Visualization | Streamlit |
| API Client | requests + openmeteo-requests |

## 📝 Development

### Adding New Components

- **Ingestion**: Add new tasks/flows in `ingestion/` directory
- **Pipeline**: Add the master orchestrator in `pipeline/`
- **Transformations**: Add dbt models in `dbt_project/models/`
- **Dashboard Pages**: Create new Streamlit pages in `dashboard/`

### Running Tests

```bash
# dbt tests
dbt test

# Python unit tests (when added)
pytest tests/
```

## 📚 Documentation

See `docs/` directory for detailed documentation, architecture diagrams, and API references.