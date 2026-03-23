# Data Portfolio - Patiotuerca ETL

End-to-end data pipeline project to extract vehicle listings, transform them into a canonical dataset, and load them into a database, orchestrated with Apache Airflow and containerized with Docker.

## Project Goals

- Build a modular ETL pipeline in Python.
- Orchestrate ETL with Airflow (DAG + retries + logs).
- Run locally in Docker with reproducible setup.
- Prepare architecture to swap data sources (scraping/API) and storage backends (SQLite/PostgreSQL).
- Showcase portfolio-ready engineering practices.
- Persist ETL outputs in both SQLite (local fallback) and PostgreSQL (portfolio target DB).

## Tech Stack

- Python: `requests`, `pandas`, `sqlalchemy`, `beautifulsoup4`
- Apache Airflow
- PostgreSQL:
  - Airflow metadata DB (`airflow`)
  - App target DB (`portfolio`)
- SQLite (local app DB fallback)
- Docker / Docker Compose

## Architecture

```mermaid
flowchart LR
  AF[Airflow DAG: etl_patiotuerca] --> E[Connector: PatiotuercaConnector]
  E --> T[Transformer: PatiotuercaTransformer]
  T --> J[JSON Snapshot]
  T --> L1[SQLiteLoader]
  T --> L2[PostgresLoader]
  L1 --> DB1[(SQLite: patiotuerca.sqlite)]
  L2 --> DB2[(PostgreSQL: portfolio.patiotuerca_vehicles)]
  AF --> META[(Airflow Metadata DB - PostgreSQL/airflow)]
```

## Quick start (local, no Airflow)

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
# source venv/bin/activate

pip install -r requirements.txt
python main.py
# or 
python main.py --max_urls_counter=1 --max_data_length=10 \
  --sqlite_db_url sqlite:///patiotuerca.sqlite \
  --postgres_db_url postgresql+psycopg2://app_user:app_pass@localhost:5434/portfolio \
  --table_name patiotuerca_vehicles
```

Outputs:

- patiotuerca.json (raw snapshot)
- Rows in patiotuerca.sqlite
- Rows in PostgreSQL table patiotuerca_vehicles

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

**4. PostgreSQL checks**

Connect with pgAdmin:
- host: localhost
- port: 5434
- user: app_user
- password: app_pass
- db: portfolio

### Validate inserts:
SELECT COUNT(*) FROM patiotuerca_vehicles;

**4. Evidence (portfolio)**

- Screenshot: `docs/screenshots/airflow_dag_success.png`
- DAG run logs (success)
- PostgreSQL row-count proof (SELECT COUNT(*))
