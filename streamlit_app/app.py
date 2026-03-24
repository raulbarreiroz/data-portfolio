"""
Read-only Patiotuerca listings dashboard.
Set DATABASE_URL to local Postgres or AWS RDS (same schema as ETL load).
Optional: Streamlit secrets.toml key DATABASE_URL for local deploys.
"""

from __future__ import annotations

import os
import re
from typing import Any

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Default matches docker-compose app-postgres port on host (see README).
_DEFAULT_URL = "postgresql+psycopg2://app_user:app_pass@127.0.0.1:5434/portfolio"


def _table_name() -> str:
    raw = os.environ.get("PATIOTUERCA_TABLE", "patiotuerca_vehicles")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", raw):
        raise ValueError("Invalid PATIOTUERCA_TABLE")
    return raw


def _database_url() -> str:
    env = os.environ.get("DATABASE_URL")
    if env:
        return env
    try:
        return str(st.secrets["DATABASE_URL"])
    except Exception:
        return _DEFAULT_URL


@st.cache_resource
def _engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True)


@st.cache_data(ttl=60)
def _row_count(engine_url: str, table: str) -> int:
    eng = _engine(engine_url)
    sql = text(f"SELECT COUNT(*) AS n FROM {table}")
    with eng.connect() as conn:
        return int(pd.read_sql(sql, conn).iloc[0]["n"])


@st.cache_data(ttl=120)
def _brand_options(engine_url: str, table: str) -> list[str]:
    eng = _engine(engine_url)
    sql = text(
        f"SELECT DISTINCT brand FROM {table} "
        "WHERE brand IS NOT NULL AND brand <> '' ORDER BY brand"
    )
    with eng.connect() as conn:
        df = pd.read_sql(sql, conn)
    return [str(x) for x in df["brand"].tolist()]


@st.cache_data(ttl=60)
def _fetch_listings(
    engine_url: str,
    table: str,
    brand: str | None,
    limit: int,
) -> pd.DataFrame:
    eng = _engine(engine_url)
    if brand:
        sql = text(
            f"SELECT title, brand, model, year, price, location, mileage, scraped_at "
            f"FROM {table} WHERE brand = :brand ORDER BY scraped_at DESC NULLS LAST "
            "LIMIT :limit"
        )
        params: dict[str, Any] = {"brand": brand, "limit": limit}
    else:
        sql = text(
            f"SELECT title, brand, model, year, price, location, mileage, scraped_at "
            f"FROM {table} ORDER BY scraped_at DESC NULLS LAST LIMIT :limit"
        )
        params = {"limit": limit}
    with eng.connect() as conn:
        return pd.read_sql(sql, conn, params=params)


def main() -> None:
    st.set_page_config(page_title="Patiotuerca listings", layout="wide")
    st.title("Patiotuerca listings (read-only)")

    table = _table_name()
    url = _database_url()

    with st.sidebar:
        st.subheader("Connection")
        st.caption("Uses `DATABASE_URL` env or Streamlit `secrets.toml`.")
        masked = url.split("@")[-1] if "@" in url else url
        st.text(f"…@{masked}")

    try:
        n = _row_count(url, table)
    except Exception as e:
        st.error("Could not connect or query the database.")
        st.code(str(e), language="text")
        st.stop()

    c1, c2, c3 = st.columns(3)
    c1.metric("Rows in table", f"{n:,}")

    brands = _brand_options(url, table)
    brand_pick = st.selectbox("Brand filter", options=["(all)"] + brands)

    limit = st.slider("Rows to show", min_value=20, max_value=500, value=100, step=20)

    brand_filter: str | None = None if brand_pick == "(all)" else brand_pick
    df = _fetch_listings(url, table, brand_filter, limit)
    c2.metric("Shown", len(df))
    c3.metric("Table", table)

    st.dataframe(df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
