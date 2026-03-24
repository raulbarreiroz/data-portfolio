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

## 1. Build and start

```bash
docker compose down -v
docker compose build --no-cache
docker compose up airflow-init
docker compose up -d
```

## 2. Open Airflow UI

- URL: `http://127.0.0.1:8081`
- Default user:
  - username: `airflow`
  - password: `airflow`

## 3. Run the DAG

1. Unpause DAG `etl_patiotuerca`
2. **Trigger DAG** (play)
3. Verify tasks `extract_task → transform_task → load_task` are green
4. Open task logs for each step; confirm DB / file updates as expected
5. PostgreSQL checks

### 4.1 Start app-postgres container (setup before pgAdmin)

```bash
docker compose up -d app-postgres
docker compose ps
```

### 4.2 Create the portfolio database (first time only)

```bash
docker exec -it data-portfolio-app-postgres-1 psql -U app_user -d postgres -c "CREATE DATABASE portfolio;"
```

#### Optional check:

```bash
docker exec -it data-portfolio-app-postgres-1 psql -U app_user -d postgres -c "\l"
```

### Connect with pgAdmin:

- Host: localhost
- Port: 5434
- Maintenance DB: postgres (or portfolio)
- Username: app_user
- Password: app_pass

### Validate inserts:

```bash
SELECT COUNT(*) FROM patiotuerca_vehicles;
```

## AWS S3 Staging (Step 4)

S3 staging is integrated in the Airflow DAG through `stage_to_s3_task`.

### What it does

- Uploads raw extracted payload to S3:
  - `patiotuerca/raw/<timestamp>.json`
- Uploads transformed payload to S3:
  - `patiotuerca/processed/<timestamp>.json`

### Why this is useful

- Keeps immutable raw history for reprocessing and auditing.
- Separates staging storage (S3) from serving storage (PostgreSQL).
- Improves pipeline observability and recovery options.

### Required environment variables

These values are loaded in Airflow containers via `.env`:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`
- `S3_BUCKET`

Example `.env`:

```bash
AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_SECRET_ACCESS_KEY
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=rb-data-portfolio-dev
```

#### Docker Compose requirement

In x-airflow-common, ensure:

env_file: [".env"]

Then recreate Airflow services:

```bash
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
```

#### How to validate

1. Trigger DAG etl_patiotuerca.
2. Verify stage_to_s3_task is green.
3. Check S3 bucket contains:

- `patiotuerca/raw/<timestamp>.json`
- `patiotuerca/processed/<timestamp>.json`

1. Optional CLI check:

```bash
aws s3 ls s3://rb-data-portfolio-dev/patiotuerca/ --recursive
```

## 4. Evidence (portfolio)

- Screenshot: `docs/screenshots/airflow_dag_success.png`
- DAG run logs (success)
- PostgreSQL row-count proof (SELECT COUNT(*))
- Short note on IAM least-privilege permissions used.

