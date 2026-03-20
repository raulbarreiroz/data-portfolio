# Data Portfolio - Patiotuerca ETL

End-to-end data pipeline project to extract vehicle listings, transform them into a canonical dataset, and load them into a database, orchestrated with Apache Airflow and containerized with Docker.

## Project Goals

- Build a modular ETL pipeline in Python.
- Orchestrate ETL with Airflow (DAG + retries + logs).
- Run locally in Docker with reproducible setup.
- Prepare architecture to swap data sources (scraping/API) and storage backends (SQLite/PostgreSQL).
- Showcase portfolio-ready engineering practices.

## Tech Stack

- Python: `requests`, `pandas`, `sqlalchemy`, `beautifulsoup4`
- Apache Airflow
- PostgreSQL (Airflow metadata DB)
- SQLite (current app target DB; PostgreSQL app DB is next step)
- Docker / Docker Compose

## Architecture

```mermaid
flowchart LR
  AF[Airflow DAG: etl_patiotuerca] --> E[Connector: PatiotuercaConnector]
  E --> T[Transformer: PatiotuercaTransformer]
  T --> J[JSON Snapshot]
  T --> L[Loader: SQLiteLoader]
  L --> DB[(App DB)]
  AF --> META[(Airflow Metadata DB - PostgreSQL)]
```

## Quick start (local, no Airflow)

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
# source venv/bin/activate

pip install -r requirements.txt
python main.py --max_urls_counter=1 --max_data_length=10
```

Outputs: `patiotuerca.json` (raw snapshot) and rows in `patiotuerca.sqlite` (if loader runs).

## Airflow flow (Docker)

**1. Build and start**

```bash
docker compose down -v
docker compose build --no-cache
docker compose up airflow-init
docker compose up -d
```

**2. Open Airflow UI**

- URL: `http://127.0.0.1:8081`
- Default user:
  - username: `airflow`
  - password: `airflow`

**3. Run the DAG**

1. Unpause DAG `etl_patiotuerca`
2. **Trigger DAG** (play)
3. Verify tasks `extract_task → transform_task → load_task` are green
4. Open task logs for each step; confirm DB / file updates as expected

**4. Evidence (portfolio)**

- Screenshot: `docs/screenshots/airflow_dag_success.png`