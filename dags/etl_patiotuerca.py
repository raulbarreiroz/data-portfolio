from typing import Any, Dict, List
from datetime import datetime, timedelta
import json

import pandas as pd
from airflow.decorators import dag, task

from src.connectors.patiotuerca.connector import PatiotuercaConnector
from src.transforms.patiotuerca import PatiotuercaTransformer
from src.loaders.sqlite_loader import SQLiteLoader

@dag(
    dag_id='etl_patiotuerca',
    start_date=datetime(2026, 3, 19),
    schedule="0 */2 * * *", # every 2 hours
    #schedule="*/10 * * * *", # every 10 minutes
    #schedule='@daily',
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=['etl', 'patiotuerca', 'portfolio'],
)
def etl_patiotuerca() -> None:
    @task
    def extract_task(
        max_urls_counter: int = 1,
        max_data_length: int = 10,
        timeout: int = 10,
    ) -> str:
        connector = PatiotuercaConnector()
        rows: List[Dict[str, Any]] = connector.extract(
            max_urls_counter=max_urls_counter,
            max_data_length=max_data_length,
            timeout=timeout,
        )
        return json.dumps(rows, ensure_ascii=False, indent=2)

    @task
    def transform_task(rows_json: str) -> str:
        rows: List[Dict[str, Any]] = json.loads(rows_json)
        df_raw: pd.DataFrame = pd.DataFrame(rows)
        transformer = PatiotuercaTransformer()
        df_final: pd.DataFrame = transformer.transform(df_raw)
        return df_final.to_json(orient="records", force_ascii=False)

    @task
    def load_task(
        df_json: str,
        # db_url: str = "sqlite:///patiotuerca.sqlite", # sqlite
        db_url: str = "postgresql+psycopg2://app_user:app_pass@app-postgres:5432/portfolio",
        table_name: str = "patiotuerca_vehicles",
    ) -> int:
        records: List[Dict[str, Any]] = json.loads(df_json)
        df: pd.DataFrame = pd.DataFrame(records)
        loader = SQLiteLoader()
        loader.load(df, db_url=db_url, table_name=table_name)
        return int(len(df))

    raw: str = extract_task()
    transformed: str = transform_task(raw)
    _loaded: int = load_task(transformed)

etl_patiotuerca()