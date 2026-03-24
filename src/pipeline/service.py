from collections.abc import Callable
from typing import Any

import pandas as pd

from src.connectors.base import SourceConnector


def run_pipeline(
    connector: SourceConnector,
    transform_fn: Callable[[pd.DataFrame], pd.DataFrame],
    load_json_fn: Callable[[list[dict[str, Any]], str], None],
    load_sql_fn: Callable[[pd.DataFrame, str, str], None],
    *,
    max_urls_counter: int,
    max_data_length: int,
    timeout: int,
    output_json: str,
    db_url: str,
    table_name: str,
) -> dict[str, Any]:
    raw_rows = connector.extract(
        max_urls_counter=max_urls_counter,
        max_data_length=max_data_length,
        timeout=timeout,
    )

    load_json_fn(raw_rows, output_json)

    df_raw = pd.DataFrame(raw_rows)
    df_final = transform_fn(df_raw)

    if not df_final.empty:
        load_sql_fn(df_final, db_url, table_name)

    return {
        "raw_rows": len(raw_rows),
        "loaded_rows": len(df_final),
        "db_url": db_url,
        "table": table_name,
    }
