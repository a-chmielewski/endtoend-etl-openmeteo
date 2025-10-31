# OpenMeteo ETL Pipeline

End-to-end data engineering project that extracts weather data from the OpenMeteo API, stores it in MinIO (S3-compatible storage), loads it into PostgreSQL, transforms it with dbt, and visualizes it in Metabase.

## Architecture

```
┌─────────────┐     ┌────────┐     ┌──────────┐     ┌──────────────┐     ┌─────┐     ┌──────────┐
│ OpenMeteo   │────▶│ MinIO  │────▶│   GE     │────▶│ PostgreSQL   │────▶│ dbt │────▶│ Metabase │
│ API         │     │ (S3)   │     │ Validate │     │ (Staging)    │     │     │     │          │
└─────────────┘     └────────┘     └──────────┘     └──────────────┘     └─────┘     └──────────┘
    Extract          Store          Quality Check     Load                Transform    Visualize
```

## Features

- **Extract**: Fetch hourly weather data from OpenMeteo API
- **Store**: Raw data in MinIO with date partitioning
- **Validate**: Great Expectations data quality checks (blocks bad data)
- **Load**: Upsert to PostgreSQL staging table (deduplicated)
- **Transform**: dbt models for staging and daily aggregations
- **Visualize**: Metabase dashboards
- **Orchestrate**: Airflow for scheduling (optional)

## Tech Stack

- **Data Source**: [OpenMeteo API](https://open-meteo.com/)
- **Object Storage**: MinIO
- **Data Quality**: Great Expectations
- **Database**: PostgreSQL 16
- **Transformation**: dbt
- **Orchestration**: Apache Airflow
- **Visualization**: Metabase
- **Infrastructure**: Docker Compose

## Quick Start

### 1. Prerequisites

- Docker & Docker Compose
- Python 3.9+
- dbt CLI (optional, for local development)

### 2. Setup Environment

Copy the environment template and configure:

```bash
cp env.template .env
# Edit .env with your credentials (or use defaults)
```

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 4. Start Services

```bash
docker-compose up -d
```

Services will be available at:
- **MinIO Console**: http://localhost:9001
- **Metabase**: http://localhost:3000
- **Airflow**: http://localhost:8080
- **PostgreSQL**: localhost:55432

### 5. Initialize Database

```bash
docker exec -i endtoend-etl-openmeteo-analytics-db-1 psql -U analytics -d analytics < ingestion/loader/sql/create_staging.sql
```

### 6. Run the Pipeline

**Option A: Run complete pipeline for October 2025**

```bash
python run_october_2025_pipeline.py
```

**Option B: Run each stage manually**

```bash
# Extract
cd ingestion/extractor
python fetch_october_2025.py Berlin

# Load
cd ../loader
python run_load_once.py

# Transform
cd ../../dbt
dbt run
dbt test
```

### 7. View in Metabase

1. Open http://localhost:3000
2. Connect to PostgreSQL:
   - Host: `analytics-db` (or `host.docker.internal`)
   - Port: `5432`
   - Database: `analytics`
   - User: `analytics`
   - Password: `chhHan!hhSsi9o35`
3. Query `fct_city_day` table
4. Create visualizations

## Project Structure

```
.
├── airflow/
│   └── dags/
│       └── etl_openmeteo.py          # Main ETL DAG with GE validation
├── dbt/
│   ├── models/
│   │   ├── staging/            # Staging models
│   │   │   └── stg_weather_hourly.sql
│   │   └── marts/              # Business logic models
│   │       └── fct_city_day.sql
│   └── dbt_project.yml
├── ge/                                # Great Expectations validation
│   ├── validate_raw_weather.py       # Main validation logic
│   ├── run_checkpoint.py             # CLI wrapper
│   ├── test_validation.py            # Test scenarios
│   └── README.md                     # GE documentation
├── ingestion/
│   ├── extractor/
│   │   ├── openmeteo_client.py       # API client
│   │   ├── s3_writer.py              # Write to MinIO
│   │   ├── run_once.py               # Extract last 6 hours
│   │   └── fetch_october_2025.py     # Extract October 2025
│   └── loader/
│       ├── load_to_postgres.py       # Load logic
│       ├── run_load_once.py          # Load runner
│       └── sql/
│           └── create_staging.sql    # Schema setup
├── docs/
│   └── OCTOBER_2025_GUIDE.md   # Detailed guide
├── docker-compose.yml
├── requirements.txt
└── run_october_2025_pipeline.py  # Complete pipeline runner
```

## Data Models

### Staging: `stg_weather_hourly`

Cleaned hourly weather data from the staging table.

```sql
SELECT city, timestamp, temperature_2m, precipitation, wind_speed_10m
FROM staging.weather_hourly;
```

### Marts: `fct_city_day`

Daily weather aggregates by city.

```sql
SELECT 
    city,
    day,
    AVG(temperature_2m) as temperature_2m,
    AVG(precipitation) as precipitation,
    AVG(wind_speed_10m) as wind_speed_10m
FROM staging.weather_hourly
GROUP BY city, DATE_TRUNC('day', timestamp);
```

## Configuration

### Supported Cities

Default cities configured in `fetch_october_2025.py`:

- Berlin (52.52, 13.41)
- Warsaw (52.23, 21.01)
- London (51.51, -0.13)
- Paris (48.85, 2.35)

Add more cities by editing the `CITIES` dictionary.

### Environment Variables

See `env.template` for all available configuration options.

Key variables:
- `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`: MinIO credentials
- `POSTGRES_HOST` / `POSTGRES_PORT`: Database connection
- `WEATHER_CITY`: City to fetch data for

## Usage Examples

### Fetch Data for Multiple Cities

```bash
cd ingestion/extractor
python fetch_october_2025.py all
```

### Query Daily Averages

```sql
SELECT 
    city,
    day,
    ROUND(temperature_2m::numeric, 2) as avg_temp_c,
    ROUND(precipitation::numeric, 2) as avg_precip_mm,
    ROUND(wind_speed_10m::numeric, 2) as avg_wind_kmh
FROM fct_city_day
WHERE day >= '2025-10-01' AND day < '2025-11-01'
ORDER BY city, day;
```

### Find Coldest Day in October

```sql
SELECT city, day, temperature_2m
FROM fct_city_day
WHERE day >= '2025-10-01' AND day < '2025-11-01'
ORDER BY temperature_2m ASC
LIMIT 1;
```

## Troubleshooting

### Services Not Starting

```bash
# Check logs
docker-compose logs -f

# Restart services
docker-compose down
docker-compose up -d
```

### Can't Connect to PostgreSQL

```bash
# Check if database is ready
docker exec -it endtoend-etl-openmeteo-analytics-db-1 pg_isready -U analytics

# Manual connection
docker exec -it endtoend-etl-openmeteo-analytics-db-1 psql -U analytics -d analytics
```

### MinIO Bucket Not Found

1. Open http://localhost:9001
2. Login with `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`
3. Create bucket named `raw`

### OpenMeteo API Limitations

- Free tier: ~3 months of historical data
- Rate limits apply
- For October 2025 data (future), modify script dates or use current dates for testing

## Documentation

- [October 2025 Complete Guide](docs/OCTOBER_2025_GUIDE.md) - Step-by-step walkthrough
- [OpenMeteo API Docs](https://open-meteo.com/en/docs)
- [dbt Documentation](https://docs.getdbt.com/)

## Development

### Running Tests

```bash
# dbt tests
cd dbt
dbt test

# Python tests (if you add pytest)
pytest tests/
```

### Adding New Models

1. Create SQL file in `dbt/models/`
2. Add tests in `dbt/models/schema.yml`
3. Run: `dbt run --select your_model`

### Creating Airflow DAGs

1. Add DAG file to `airflow/dags/`
2. DAG will auto-sync (volume mounted)
3. View in Airflow UI: http://localhost:8080

## Data Quality with Great Expectations

The pipeline includes **Great Expectations** validation that runs between Extract and Load phases.

### Validation Rules

- ✅ **Time field** must not be null
- ✅ **Temperature** must be between -90°C and 60°C
- ✅ **Precipitation** must be >= 0mm (non-negative)
- ✅ **Wind speed** must be >= 0 m/s (non-negative)
- ✅ **City, latitude, longitude** must not be null

### Validation Flow

```
Extract → Validate (GE) → Load
          ↓
          If validation fails:
          - Load task is BLOCKED
          - No bad data enters database
          - Airflow task fails with details
```

### Testing Validation

```bash
# Run test scenarios
python ge/test_validation.py

# See validation logic
cat ge/validate_raw_weather.py
```

For detailed documentation, see [ge/README.md](ge/README.md).

## Future Enhancements

- [ ] Add more cities and weather metrics
- [ ] Implement incremental loading in dbt
- [x] Add Great Expectations for data quality ✅
- [ ] Create pre-built Metabase dashboards
- [ ] Add alerting for pipeline failures
- [ ] Implement CDC (Change Data Capture)
- [ ] Add API for querying transformed data

## License

MIT License - feel free to use for learning and projects.

## Contributing

Contributions welcome! Please open an issue or PR.

## Contact

For questions or feedback, open an issue on GitHub.

---

**Happy Data Engineering! 🌤️📊**
