# OpenMeteo ETL Pipeline

[![CI](https://github.com/YOUR_USERNAME/endtoend-etl-openmeteo/workflows/CI/badge.svg)](https://github.com/YOUR_USERNAME/endtoend-etl-openmeteo/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)

End-to-end data engineering pipeline demonstrating modern ETL best practices with automated data quality validation. Extracts weather data from OpenMeteo API, validates with Great Expectations, loads into PostgreSQL, transforms with dbt, and visualizes in Metabase.

## Overview

This project showcases a complete data engineering workflow implementing industry-standard practices:

- **Extract-Load-Transform (ELT)** pattern with raw data lake
- **Data Quality Gates** preventing bad data from entering the warehouse
- **Incremental Processing** with partition-aware ingestion
- **Infrastructure as Code** using Docker Compose
- **Continuous Integration** with automated linting and testing
- **Transformation Testing** with dbt schema validations

## Architecture

```
┌──────────────┐
│  OpenMeteo   │  Weather API (Hourly Data)
│     API      │
└──────┬───────┘
       │ Extract
       ▼
┌──────────────┐
│    MinIO     │  S3-Compatible Object Storage
│  (Raw Lake)  │  Partitioned: /weather/ds=YYYY-MM-DD/hour=HH/
└──────┬───────┘
       │
       ▼
┌──────────────┐
│    Great     │  Data Quality Validation
│ Expectations │  • Schema validation
│              │  • Range checks
└──────┬───────┘  • Null checks
       │ ✓ Pass
       ▼
┌──────────────┐
│  PostgreSQL  │  OLAP Database
│   (Staging)  │  Table: staging.weather_hourly
└──────┬───────┘  PK: (city, timestamp)
       │
       ▼
┌──────────────┐
│     dbt      │  Data Transformation
│              │  • Staging layer (type casting)
└──────┬───────┘  • Marts layer (daily aggregations)
       │
       ▼
┌──────────────┐
│   Metabase   │  Business Intelligence
│              │  Dashboards & Analytics
└──────────────┘

Orchestration: Apache Airflow (DAG: extract → validate → load → transform)
```

## Key Features

### Data Quality Assurance
- **Pre-load validation** with Great Expectations blocks bad data before database insertion
- **Automated quality checks** on temperature ranges, null values, and data types
- **Failed validation stops pipeline** preventing data corruption

### Robust Data Ingestion
- **Idempotent loads** using upsert (ON CONFLICT) for safe reruns
- **Partition-aware storage** for efficient data management
- **Incremental processing** fetches only recent data

### Modern Tooling
- **dbt** for SQL-based transformations with version control
- **Airflow** for workflow orchestration and scheduling
- **Docker Compose** for reproducible local development
- **Automated CI/CD** with GitHub Actions

## Tech Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **API** | OpenMeteo | Weather data source |
| **Storage** | MinIO (S3) | Raw data lake with partitioning |
| **Quality** | Great Expectations 1.0+ | Pre-load data validation |
| **Database** | PostgreSQL 16 | OLAP warehouse |
| **Transform** | dbt Core 1.7+ | SQL transformations |
| **Orchestration** | Apache Airflow 2.9+ | Workflow scheduling |
| **Visualization** | Metabase | BI dashboards |
| **Infrastructure** | Docker Compose | Container orchestration |
| **CI/CD** | GitHub Actions | Automated testing |

## Quick Start

### Prerequisites

Ensure you have the following installed:

- **Docker Desktop** (or Docker Engine + Docker Compose)
- **Python 3.9 or higher**
- **Git** 

### Installation

**1. Clone the repository**

```bash
git clone https://github.com/a-chmielewski/endtoend-etl-openmeteo.git
cd endtoend-etl-openmeteo
```

**2. Install Python dependencies**

```bash
# Using pip
pip install -r requirements.txt

# Or using a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

**3. Start infrastructure services**

```bash
docker-compose up -d
```

Wait 30-60 seconds for services to initialize. Verify all services are healthy:

```bash
docker-compose ps
```

**4. Initialize the database schema**

```bash
docker exec -i endtoend-etl-openmeteo-analytics-db-1 psql -U analytics -d analytics < ingestion/loader/sql/create_staging.sql
```

**5. Create MinIO bucket (first-time setup)**

Option A - Via UI:
1. Open http://localhost:9001
2. Login with your credentials
3. Create bucket named `raw`

Option B - Via CLI:
```bash
docker exec endtoend-etl-openmeteo-minio-1 mc mb /data/raw
```



```

### Airflow Orchestration**

1. Access Airflow UI: http://localhost:8080
2. Default credentials: `admin` / `admin`
3. Enable the `etl_openmeteo` DAG
4. Trigger manually or wait for hourly schedule


### Metabase Setup

**Initial Configuration:**

1. Navigate to http://localhost:3000
2. Complete the setup wizard
3. Add PostgreSQL connection with your settings

4. Query the `fct_city_day` table to explore daily weather aggregations

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

The pipeline is pre-configured for four European cities:


| **Berlin** | 52.52°N | 13.41°E |
| **Warsaw** | 52.23°N | 21.01°E |
| **London** | 51.51°N | -0.13°W |
| **Paris**  | 48.85°N | 2.35°E  |

**Adding New Cities:**

Edit `ingestion/extractor/fetch_october_2025.py` or `airflow/dags/etl_openmeteo.py`:

```python
CITIES = {
    "Berlin": (52.52, 13.41),
    "Madrid": (40.42, -3.70),  # Add your city
}
```

### Environment Variables

Key configuration variables (see `docker-compose.yml` for complete list):

| Variable | Default | Purpose |
|----------|---------|---------|
| `MINIO_ROOT_USER` | `minioadmin` | MinIO access key |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | MinIO secret key |
| `MINIO_ENDPOINT_URL` | `http://localhost:9000` | MinIO API endpoint |
| `POSTGRES_HOST` | `localhost` | Database host |
| `POSTGRES_PORT` | `5432` | Database port (external: 55432) |
| `POSTGRES_DB` | `analytics` | Database name |
| `POSTGRES_USER` | `analytics` | Database user |
| `POSTGRES_PASSWORD` | `chhHan!hhSsi9o35` | Database password |

**Override in production:**
```bash
export POSTGRES_PASSWORD="your_secure_password"
export MINIO_ROOT_PASSWORD="your_secure_key"
```

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

### Docker Services Issues

**Problem: Services won't start**
```bash
# Check service status
docker-compose ps

# View logs for specific service
docker-compose logs -f postgres
docker-compose logs -f minio

# Full restart
docker-compose down
docker-compose up -d

# Remove volumes (CAUTION: deletes all data)
docker-compose down -v
```

**Problem: Port conflicts**
```bash
# Check what's using the port
netstat -an | grep 55432  # PostgreSQL
netstat -an | grep 9001   # MinIO

# Change ports in docker-compose.yml if needed
```

### Database Connection Issues

**Problem: Can't connect to PostgreSQL**
```bash
# Verify database is ready
docker exec -it endtoend-etl-openmeteo-analytics-db-1 pg_isready -U analytics

# Test connection
docker exec -it endtoend-etl-openmeteo-analytics-db-1 psql -U analytics -d analytics

# Check if schema exists
docker exec -it endtoend-etl-openmeteo-analytics-db-1 psql -U analytics -d analytics -c "\dn"
```

### MinIO/S3 Issues

**Problem: Bucket not found**
```bash
# Option 1: Create via UI
# 1. Open http://localhost:9001
# 2. Login with your credentials
# 3. Create bucket: raw

# Option 2: Create via command
docker exec endtoend-etl-openmeteo-minio-1 mc mb /data/raw
```

**Problem: Access denied errors**
- Verify `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` in environment
- Check bucket permissions in MinIO console
- Ensure bucket name matches code expectations (`raw`)

### Pipeline Errors

**Problem: Validation failures**
```bash
# Test validation logic
python ge/test_validation.py

# Check raw data format
# Verify S3 files contain expected structure
```

**Problem: dbt compilation errors**
```bash
# Verify dbt profile
cat ~/.dbt/profiles.yml

# Test database connection
cd dbt
dbt debug

# Compile without running
dbt compile
```

**Problem: Airflow DAG not showing**
```bash
# Check DAG folder is mounted
docker exec -it endtoend-etl-openmeteo-airflow-1 ls /opt/airflow/dags

# View Airflow logs
docker-compose logs -f airflow

# Verify Python dependencies
docker exec -it endtoend-etl-openmeteo-airflow-1 pip list
```

### API Issues

**OpenMeteo API Limitations:**
- **Free tier**: ~3 months of historical data via Archive API
- **Rate limits**: May apply for high-frequency requests
- **Future dates**: October 2025 data is for demonstration; use current dates for testing
- **Downtime**: Check [status page](https://open-meteo.com/) if API unavailable

**Problem: API returns no data**
```bash
# Test API directly
curl "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m"

# Check date ranges are valid
# Verify city coordinates are correct
```

### Performance Issues

**Problem: Slow queries**
```sql
-- Check table sizes
SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename))
FROM pg_tables WHERE schemaname = 'staging';

-- Add indexes if needed
CREATE INDEX idx_weather_city_timestamp ON staging.weather_hourly(city, timestamp);
```

**Problem: High memory usage**
- Limit dbt threads: Edit `~/.dbt/profiles.yml`, set `threads: 1`
- Reduce docker-compose resource limits
- Process cities incrementally instead of all at once

## Documentation

- [OpenMeteo API Docs](https://open-meteo.com/en/docs)
- [dbt Documentation](https://docs.getdbt.com/)

## CI/CD Pipeline

### Automated Testing

This project includes comprehensive CI/CD using GitHub Actions. On every push, the following checks run automatically:

#### Test Matrix

| Job | Purpose | Tools |
|-----|---------|-------|
| **Lint & Format** | Code quality checks | `ruff`, `black` |
| **Great Expectations Tests** | Validation framework tests | `pytest` |
| **Airflow DAG Validation** | DAG syntax and imports | Apache Airflow |
| **Ingestion Module Tests** | Import validation | Python imports |
| **dbt Compilation** | SQL syntax validation | dbt parse |

#### Running CI Checks Locally

```bash
# Lint check
ruff check .
black --check .

# Great Expectations tests
pytest ge/test_validation.py -v

# dbt validation
cd dbt && dbt parse

# Airflow DAG parsing
python -c "from airflow.dags.etl_openmeteo import dag"
```

### Workflow Configuration

See `.github/workflows/ci.yml` for the complete CI configuration.

## Development

### Running Tests

**dbt Tests:**
```bash
cd dbt
dbt test                    # Run all tests
dbt test --select marts.*  # Test marts models only
```

**Python Tests:**
```bash
pytest ge/test_validation.py -v
```

**Integration Test:**
```bash
# Test complete pipeline locally
python run_october_2025_pipeline.py
```

### Adding New dbt Models

1. Create SQL file in appropriate directory:
   - Staging: `dbt/models/staging/`
   - Marts: `dbt/models/marts/`

2. Add schema tests in `dbt/models/schema.yml`:
```yaml
models:
  - name: your_new_model
    columns:
      - name: id
        tests: [unique, not_null]
```

3. Run and test:
```bash
cd dbt
dbt run --select your_new_model
dbt test --select your_new_model
```

### Creating Custom Airflow DAGs

1. Create new Python file in `airflow/dags/`
2. Define DAG with TaskFlow API:
```python
from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="my_custom_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
) as dag:
    @task
    def my_task():
        # Your logic here
        pass
```

3. DAG will auto-sync to Airflow UI (volume mounted)
4. Verify in UI: http://localhost:8080

### Code Quality Standards

**Python:**
- Follow PEP 8 style guide
- Use type hints where applicable
- Document functions with docstrings
- Keep functions focused and modular

**SQL (dbt):**
- Use CTEs for readability
- Avoid SELECT *
- Add comments for complex logic
- Follow dbt best practices

## Data Quality Framework

### Great Expectations Integration

The pipeline implements a **data quality gate** between extraction and loading phases. All raw data is validated before entering the warehouse, preventing data corruption and ensuring analytical reliability.

#### Validation Rules

| Check | Expectation | Threshold |
|-------|------------|-----------|
| **Timestamp** | Not null | 100% |
| **Temperature** | Between -90°C and 60°C | 100% |
| **Precipitation** | ≥ 0 mm | 100% |
| **Wind Speed** | ≥ 0 m/s | 100% |
| **Location Data** | City, latitude, longitude not null | 100% |
| **Timezone** | Not null | 100% |

#### Pipeline Behavior

```
┌─────────┐     ┌──────────┐     ┌──────┐
│ Extract │────▶│ Validate │────▶│ Load │
└─────────┘     └────┬─────┘     └──────┘
                     │
                     │ ✓ PASS: Continue to Load
                     │
                     │ ✗ FAIL: Block Load
                     └────▶ Airflow Task Fails
                           └────▶ Alert Triggered
                                 └────▶ No Bad Data in DB
```

#### Testing Data Quality

**Run validation test scenarios:**
```bash
python ge/test_validation.py
```

**Inspect validation logic:**
```bash
cat ge/validate_raw_weather.py
```

**Manual validation of S3 data:**
```python
from ge.validate_raw_weather import validate_weather_data

results = {
    "Warsaw": ["s3://raw/weather/Warsaw/ds=2025-10-31/hour=12/file.json"]
}

try:
    validate_weather_data(results)
    print("✓ Data quality checks passed")
except ValueError as e:
    print(f"✗ Validation failed: {e}")
```

See [ge/README.md](ge/README.md) for complete documentation.

## Production Deployment

### Considerations

**Security:**
- Change default passwords
- Use secrets management (AWS Secrets Manager, Vault)
- Enable SSL/TLS for all connections
- Implement network policies

**Scalability:**
- Deploy on Kubernetes for auto-scaling
- Use managed services (RDS, S3, Managed Airflow)
- Implement connection pooling
- Optimize dbt model materialization strategies

**Monitoring:**
- Set up application metrics
- Configure Airflow SLA alerts
- Implement data quality dashboards
- Log aggregation

**Reliability:**
- Configure backup strategies
- Implement disaster recovery
- Set up multi-region replication
- Define RPO/RTO requirements

## Contributing

Contributions are welcome! This project is designed for learning and demonstrating data engineering patterns.

### How to Contribute

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/your-feature`
3. **Make your changes** following the code quality standards
4. **Run tests**: `ruff check . && pytest`
5. **Commit**: `git commit -m "Add: your feature description"`
6. **Push**: `git push origin feature/your-feature`
7. **Open a Pull Request**

### Contribution Ideas

- Add support for new weather APIs
- Implement additional dbt models
- Create Metabase dashboard templates
- Improve documentation
- Add unit tests
- Optimize query performance

## License

This project is licensed under the MIT License.

You are free to use, modify, and distribute this project for personal or commercial purposes.

## Acknowledgments

- [OpenMeteo](https://open-meteo.com/) for providing free weather data
- [dbt](https://www.getdbt.com/) for modern data transformation
- [Great Expectations](https://greatexpectations.io/) for data quality framework
- The open-source data engineering community

## Support

**Questions or Issues?**
- Open an [issue](https://github.com/YOUR_USERNAME/endtoend-etl-openmeteo/issues)
- Check existing issues for solutions
- Review documentation in the `/docs` folder

**Learning Resources:**
- [dbt Learn](https://courses.getdbt.com/)
- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Great Expectations Tutorials](https://docs.greatexpectations.io/docs/)

---

**Built with modern data engineering best practices**
