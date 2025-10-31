# How to Run (Dev)

1. cp .env.example .env
2. docker compose up -d minio analytics-db metabase
3. python ingestion/extractor/run_once.py
4. Load to Postgres: run loader script.
5. dbt run && dbt test
6. Open Metabase: http://localhost:3000
7. Start Airflow: docker compose up -d airflow-db airflow-init airflow-scheduler airflow-webserver
