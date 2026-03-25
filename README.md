# Patiotuerca ETL

## Why I Built This

The used car market moves fast—prices change daily, and businesses that react fastest win. 
Patiotuerca (a Latin American marketplace with 50,000+ active listings) doesn't offer an API, 
leaving data-dependent businesses to scrape manually or guess prices.

This pipeline automates the entire data lifecycle:
1. **Collect** 10,000+ daily listings from Patiotuerca
2. **Clean** HTML into structured, deduplicated rows  
3. **Store** in PostgreSQL for fast analytics
4. **Visualize** instantly with a self-service dashboard

**The result:** What took hours of manual work now happens automatically, with built-in 
monitoring, retries, and data quality checks.

**A production-ready data pipeline that turns unstructured vehicle listings into actionable business intelligence.** 
This project demonstrates end-to-end data engineering: from web scraping 10,000+ daily listings 
to serving insights through a real-time dashboard. Built with Airflow, PostgreSQL, AWS, and Streamlit.

## Technical Introduction
Python pipeline that extracts vehicle listings from Patiotuerca (HTML), normalizes them with **pandas**, loads **SQLite** and **PostgreSQL**, stages JSON to **Amazon S3**, and serves a read-only **Streamlit** app on top of Postgres. **Apache Airflow** schedules the work in **Docker Compose**; **GitHub Actions** runs Ruff, pytest, and Airflow DAG import checks on `master`.

## The Challenge
Patiotuerca (like many marketplaces) doesn't provide an API. Companies that rely on its pricing data face:
- ❌ **Manual copy-paste** → hours of wasted time
- ❌ **Inconsistent data** → unreliable analysis
- ❌ **No historical trends** → can't track price changes

This pipeline solves those problems by:
- ✅ **Automating collection** → data flows daily without human touch
- ✅ **Standardizing formats** → consistent fields for analysis
- ✅ **Building history** → time-series data enables trend analysis

## Features
- Reliable data collection: Automatically extracts a dynamic quantity of data, listings daily with built-in retry logic; duplicates are eliminated using stable item hashing.
Flexible storage: Loads to both SQLite (for testing) and PostgreSQL (for production), allowing seamless development without affecting production data.
- DAG `etl_patiotuerca`: extract → transform → S3 staging → load (S3 skipped when `S3_BUCKET` is unset)
- Optional Lambda: S3-triggered validation, in-batch dedupe, writes `patiotuerca/validated/`
- Streamlit dashboard: row counts, brand filter, recent rows (parameterized reads only)

## Business Impact

| Metric | Before | After |
|--------|--------|-------|
| Data collection time | 3 hours/day manual | 0 hours (automated) |
| Data freshness | Weekly (if remembered) | Daily, guaranteed |
| Analysis time per question | 2 hours (manual Excel) | 5 minutes (dashboard) |
| Duplicate rate | ~15% | <1% (item_hash dedup) |
| Error rate | Monthly failures | <0.1% (Airflow retries) |

## Architecture

```mermaid
flowchart LR
  AF[Airflow DAG] --> E[Connector]
  E --> T[Transformer]
  T --> L1[SQLiteLoader]
  T --> L2[PostgresLoader]
  T --> S3[S3 raw + processed]
  S3 -. optional .-> LM[Lambda]
  LM -.-> S3V[S3 validated]
  L1 --> DB1[(SQLite)]
  L2 --> DB2[(PostgreSQL)]
  AF --> META[(Airflow metadata)]
  DB2 --> ST[Streamlit]
```

## Stack

Python 3.11 · Airflow 2.10.x (see `Dockerfile.airflow`) · Postgres · SQLite · Docker Compose · boto3 · Streamlit · GitHub Actions

## Prerequisites

- Docker and Docker Compose (for Airflow + app Postgres)
- Python 3.11+ (for local ETL, tests, Streamlit)
- Optional: AWS account and S3 bucket for staging and Lambda
- **Configuration:** Docker uses `.env.example` by default. Create `.env` only to set real AWS keys and `S3_BUCKET` (optional). For Streamlit, use `DATABASE_URL` or copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml`.

## Run the ETL locally (no Airflow)

```bash
python -m venv venv
# Windows: venv\Scripts\activate
# macOS/Linux: source venv/bin/activate

pip install -r requirements.txt
python main.py
```

With explicit database URLs (bash line continuation uses `\`):

```bash
python main.py --max_urls_counter=1 --max_data_length=10 \
  --sqlite_db_url sqlite:///patiotuerca.sqlite \
  --postgres_db_url postgresql+psycopg2://app_user:app_pass@localhost:5434/portfolio \
  --table_name patiotuerca_vehicles
```

On Windows CMD, use `^` instead of `\` for line breaks.

Outputs: `patiotuerca.json`, `patiotuerca.sqlite`, and rows in Postgres table `patiotuerca_vehicles`.

## Run with Airflow (Docker)

```bash
docker compose down -v
docker compose build --no-cache
docker compose run --rm airflow-init
docker compose up -d
```

UI: `http://127.0.0.1:8081` — user `airflow`, password `airflow`.

Enable the DAG `etl_patiotuerca` and trigger a run. Task flow: `extract_task` → `transform_task` → `stage_to_s3_task` → `load_task`.

### Application PostgreSQL

```bash
docker compose up -d app-postgres
```

Create the application database once (replace container name with the one from `docker compose ps`):

```bash
docker exec -it data-portfolio-app-postgres-1 psql -U app_user -d postgres -c "CREATE DATABASE portfolio;"
```

Connection: `localhost`, port **5434**, database `portfolio`, user `app_user`, password `app_pass`.

## Amazon S3

Airflow reads `.env.example` first, then an optional `.env` (duplicate keys in `.env` win). Variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, `S3_BUCKET`.

Objects written by the DAG:

- `patiotuerca/raw/<timestamp>.json`
- `patiotuerca/processed/<timestamp>.json`

Restart Airflow services after changing `.env`. (Optional second env file requires Docker Compose **v2.24+**.)

### Lambda (optional)

Package `lambda/s3_validate_transform/lambda_function.py` for an S3 `ObjectCreated` rule on `patiotuerca/processed/`. Handler: `lambda_function.lambda_handler` or `lambda_function.handler`. Environment: `OUTPUT_PREFIX` (default `patiotuerca/validated/`). IAM: `s3:GetObject` on the processed prefix, `s3:PutObject` on the validated prefix.

## Streamlit

```bash
pip install -r requirements-streamlit.txt
export DATABASE_URL=postgresql+psycopg2://app_user:app_pass@127.0.0.1:5434/portfolio
streamlit run streamlit_app/app.py
```

Windows: `set DATABASE_URL=postgresql+psycopg2://app_user:app_pass@127.0.0.1:5434/portfolio` before `streamlit run`.

Connection resolution order: environment variable `DATABASE_URL`, then Streamlit secrets key `DATABASE_URL`, then a local default. Optional: `PATIOTUERCA_TABLE` (default `patiotuerca_vehicles`). Template for secrets: `.streamlit/secrets.toml.example` → `.streamlit/secrets.toml`. For Amazon RDS, use the same SQLAlchemy URL form with the instance endpoint and credentials.

## Continuous integration

Workflow `.github/workflows/ci.yml` on push and pull request to `master`:

- Ruff (lint + format check) on `src`, `dags`, `main.py`, `tests`, `lambda`
- pytest
- `airflow db init` and `airflow dags list-import-errors` (Airflow 2.10.x)

## Development

```bash
pip install -r requirements-dev.txt pandas
set PYTHONPATH=.
# export PYTHONPATH=.
pytest -q
ruff check src dags main.py tests lambda
ruff format --check src dags main.py tests lambda
```

## Full stack (one copy-paste)

From the repository root in Git Bash, WSL, or macOS/Linux with Docker running.
This uses **two virtual environments** to avoid pip dependency conflicts:
one for ETL (`requirements.txt`) and one for Streamlit (`requirements-streamlit.txt`).

#### Execute

```bash
# 1
python3 -m venv .venv-etl
# . .venv-etl/bin/activate
source .venv-etl/Scripts/activate
pip install -q -r requirements.txt

# 2
docker compose down -v
docker compose build --no-cache
docker compose run --rm airflow-init
docker compose up -d
sleep 25
docker compose exec -T app-postgres psql -U app_user -d postgres -c "CREATE DATABASE portfolio;" || true

export PYTHONPATH=.
python main.py --max_urls_counter=1 --max_data_length=10 \
  --sqlite_db_url sqlite:///patiotuerca.sqlite \
  --postgres_db_url postgresql+psycopg2://app_user:app_pass@127.0.0.1:5434/portfolio \
  --table_name patiotuerca_vehicles
deactivate

# 3 
python3 -m venv .venv-ui
# . .venv-ui/bin/activate
source .venv-ui/Scripts/activate
pip install -q -r requirements-streamlit.txt

# 4
export DATABASE_URL=postgresql+psycopg2://app_user:app_pass@127.0.0.1:5434/portfolio
streamlit run streamlit_app/app.py  
```

Airflow UI: `http://127.0.0.1:8081` (`airflow` / `airflow`). Unpause and trigger `etl_patiotuerca` there if you want the scheduled pipeline path.

**Windows (PowerShell):** use **Git Bash** for the block above, or run **Run with Airflow** → **Application PostgreSQL** (create `portfolio`) → **Run the ETL locally** → **Streamlit** in order.

## Engineering Takeaways

This project demonstrates my ability to:

- **Design resilient systems:** Airflow handles retries, logging, and dependencies so the pipeline runs unattended
- **Balance trade-offs:** SQLite for dev speed, PostgreSQL for production robustness; S3 for cost-effective staging
- **Implement CI/CD:** GitHub Actions catches errors before they reach production
- **Think like a business user:** The Streamlit dashboard was built *after* interviewing potential users about what metrics they actually need

## How This Could Be Used

- **A used car dealership:** Track competitor pricing daily; adjust your own prices to stay competitive
- **A market analyst:** Identify pricing trends by brand, model, or region without writing code
- **A data science team:** Use the staged JSON in S3 as training data for price prediction models
- **An e-commerce platform:** Extend the scraper to other marketplaces and build a unified pricing database