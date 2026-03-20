import argparse
import pandas as pd
import json

from src.connectors.patiotuerca.connector import PatiotuercaConnector
from src.transforms.patiotuerca import PatiotuercaTransformer
from src.loaders.sqlite_loader import SQLiteLoader
from src.pipeline.service import run_pipeline

# args params
def parse_args() -> argparse.Namespace:    
    p = argparse.ArgumentParser()    
    p.add_argument("--max_urls_counter",dest="max_urls_counter_kw",type=int,default=1)
    p.add_argument("--max_data_length",dest="max_data_length_kw",type=int,default=10)
    p.add_argument("--output_json", type=str, default="patiotuerca.json")
    p.add_argument("--db_url", type=str, default="sqlite:///patiotuerca.sqlite")
    p.add_argument("--table_name", type=str, default="patiotuerca_vehicles")
    p.add_argument("--timeout", type=int, default=30)
    return p.parse_args()

def save_json(rows, output_json: str) -> None:
    with open(output_json, "w", encoding="utf-8") as file:
        json.dump(rows, file, ensure_ascii=False, indent=2)

def main() -> None:
    args = parse_args()
    
    connector = PatiotuercaConnector()
    transformer = PatiotuercaTransformer()
    loader = SQLiteLoader()

    result = run_pipeline(
        connector=connector,
        transform_fn=transformer.transform,
        load_json_fn=save_json,
        load_sql_fn=loader.load,
        max_urls_counter=args.max_urls_counter_kw,
        max_data_length=args.max_data_length_kw,
        timeout=args.timeout,
        output_json=args.output_json,
        db_url=args.db_url,
        table_name=args.table_name,
    )
    
    print("===============================================")
    print(f"raw_rows={result['raw_rows']}")
    print(f"loaded_rows={result['loaded_rows']}")
    print(f"db_url={result['db_url']}")
    print(f"table={result['table']}")
    print("===============================================")

if __name__ == "__main__":
    main()