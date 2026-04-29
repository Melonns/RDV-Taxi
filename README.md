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