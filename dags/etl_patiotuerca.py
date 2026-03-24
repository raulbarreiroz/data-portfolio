import json
import os
from datetime import datetime, timedelta
from typing import Any

import boto3
import pandas as pd
from airflow.decorators import dag, task

from src.connectors.patiotuerca.connector import PatiotuercaConnector
from src.loaders.postgres_loader import PostgresLoader
from src.loaders.sqlite_loader import SQLiteLoader
from src.transforms.patiotuerca import PatiotuercaTransformer


@dag(
    dag_id="etl_patiotuerca",
    start_date=datetime(2026, 3, 19),
    schedule="0 */2 * * *",  # every 2 hours
    # schedule="*/10 * * * *", # every 10 minutes
    # schedule='@daily',
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["etl", "patiotuerca", "portfolio", "s3"],
)
def etl_patiotuerca() -> None:
    @task
    def extract_task(
        max_urls_counter: int = 1,
        max_data_length: int = 10,
        timeout: int = 10,
    ) -> str:
        connector = PatiotuercaConnector()
        rows: list[dict[str, Any]] = connector.extract(
            max_urls_counter=max_urls_counter,
            max_data_length=max_data_length,
            timeout=timeout,
        )
        return json.dumps(rows, ensure_ascii=False, indent=2)

    @task
    def transform_task(rows_json: str) -> str:
        rows: list[dict[str, Any]] = json.loads(rows_json)
        df_raw: pd.DataFrame = pd.DataFrame(rows)
        transformer = PatiotuercaTransformer()
        df_final: pd.DataFrame = transformer.transform(df_raw)
        return df_final.to_json(orient="records", force_ascii=False)

    @task
    def stage_to_s3_task(
        raw_json: str,
        transformed_json: str,
        s3_bucket: str = "",
        s3_prefix: str = "patiotuerca",
    ) -> str:
        bucket = s3_bucket or os.getenv("S3_BUCKET", "")
        region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        if not bucket:
            return "skipped:no_bucket"
        client = boto3.client("s3", region_name=region)
        ts = datetime.now().strftime("%Y%m%dT%H%M%SZ")
        raw_key = f"{s3_prefix}/raw/{ts}.json"
        transformed_key = f"{s3_prefix}/processed/{ts}.json"
        client.put_object(
            Bucket=bucket,
            Key=raw_key,
            Body=raw_json.encode("utf-8"),
            ContentType="application/json",
        )
        client.put_object(
            Bucket=bucket,
            Key=transformed_key,
            Body=transformed_json.encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{bucket}/{s3_prefix}/"

    @task
    def load_task(
        df_json: str,
        sqlite_db_url: str = "sqlite:////opt/airflow/patiotuerca.sqlite",
        postgres_db_url: str = "postgresql+psycopg2://app_user:app_pass@app-postgres:5432/portfolio",
        table_name: str = "patiotuerca_vehicles",
    ) -> int:
        records: list[dict[str, Any]] = json.loads(df_json)
        df = pd.DataFrame(records)
        sqlite_loader = SQLiteLoader()
        postgres_loader = PostgresLoader()
        sqlite_loader.load(df, db_url=sqlite_db_url, table_name=table_name)
        postgres_loader.load(df, db_url=postgres_db_url, table_name=table_name)
        return int(len(df))

    raw = extract_task()
    transformed = transform_task(raw)
    _s3 = stage_to_s3_task(raw, transformed)
    _loaded = load_task(transformed)


etl_patiotuerca()
