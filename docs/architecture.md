# Architecture
```mermaid
flowchart LR
  A[Open-Meteo API] --> B[MinIO (raw)]
  B --> C[Postgres (staging)]
  C --> D[dbt (marts)]
  D --> E[Metabase (BI)]
  B --> F[Great Expectations (DQ)]
  C --> F
  G[Airflow] --> A
  G --> B
  G --> C
  G --> D
  G --> F
  ```