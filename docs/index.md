# OpenMeteo ETL (Free Stack)
End-to-end pipeline: API → MinIO → Postgres → dbt → Metabase → Airflow → Great Expectations.

## Highlights
- Incremental ingestion with watermarking
- dbt models + tests
- Metabase dashboard with filters
- Airflow DAGs (hourly)
- Data quality checks (GX)
