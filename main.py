import argparse
import json
from typing import Any, Dict, List

import pandas as pd

from src.connectors.patiotuerca.connector import PatiotuercaConnector
from src.transforms.patiotuerca import PatiotuercaTransformer
from src.loaders.sqlite_loader import SQLiteLoader
from src.loaders.postgres_loader import PostgresLoader


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--max_urls_counter", dest="max_urls_counter_kw", type=int, default=1)
    p.add_argument("--max_data_length", dest="max_data_length_kw", type=int, default=10)
    p.add_argument("--output_json", type=str, default="patiotuerca.json")
    p.add_argument("--table_name", type=str, default="patiotuerca_vehicles")
    p.add_argument("--timeout", type=int, default=30)

    p.add_argument("--sqlite_db_url", type=str, default="sqlite:///patiotuerca.sqlite")
    p.add_argument(
        "--postgres_db_url",
        type=str,
        default="postgresql+psycopg2://app_user:app_pass@localhost:5434/portfolio",
    )
    return p.parse_args()


def save_json(rows: List[Dict[str, Any]], output_json: str) -> None:
    with open(output_json, "w", encoding="utf-8") as file:
        json.dump(rows, file, ensure_ascii=False, indent=2)


def main() -> None:
    args = parse_args()

    connector = PatiotuercaConnector()
    transformer = PatiotuercaTransformer()

    rows = connector.extract(
        max_urls_counter=args.max_urls_counter_kw,
        max_data_length=args.max_data_length_kw,
        timeout=args.timeout,
    )

    save_json(rows, args.output_json)

    df_raw = pd.DataFrame(rows)
    df_final = transformer.transform(df_raw)

    sqlite_loader = SQLiteLoader()
    postgres_loader = PostgresLoader()

    sqlite_loader.load(df_final, db_url=args.sqlite_db_url, table_name=args.table_name)
    postgres_loader.load(df_final, db_url=args.postgres_db_url, table_name=args.table_name)

    print("===============================================")
    print(f"raw_rows={len(rows)}")
    print(f"loaded_rows={len(df_final)}")
    print(f"sqlite_db_url={args.sqlite_db_url}")
    print(f"postgres_db_url={args.postgres_db_url}")
    print(f"table={args.table_name}")
    print("===============================================")

if __name__ == "__main__":
    main()