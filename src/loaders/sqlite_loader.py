import sqlite3
import pandas as pd


def _sqlite_path(db_url: str) -> str:
    if db_url.startswith("sqlite:///"):
        return db_url.replace("sqlite:///", "", 1)
    if db_url.startswith("sqlite://"):
        return db_url.replace("sqlite://", "", 1)
    raise ValueError(f"Only sqlite db_url supported for now. Got: {db_url}")


def _ensure_table(conn: sqlite3.Connection, table_name: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_hash TEXT NOT NULL UNIQUE,
            scraped_at TEXT NOT NULL,
            inserted_at TEXT NOT NULL,
            source_url TEXT,
            title TEXT,
            image TEXT,
            fullPrice TEXT,
            price TEXT,
            year INTEGER,
            plan TEXT,
            brand TEXT,
            model TEXT,
            location TEXT,
            type TEXT,
            mileage INTEGER,
            raw_json TEXT NOT NULL
        );
        """
    )


class SQLiteLoader:
    def load(self, df: pd.DataFrame, db_url: str, table_name: str) -> None:
        if df.empty:
            return

        sqlite_path = _sqlite_path(db_url)
        conn = sqlite3.connect(sqlite_path)
        try:
            _ensure_table(conn, table_name)

            insert_cols = [
                "item_hash", "scraped_at", "inserted_at", "source_url", "title", "image",
                "fullPrice", "price", "year", "plan", "brand", "model", "location",
                "type", "mileage", "raw_json",
            ]

            missing = [c for c in insert_cols if c not in df.columns]
            if missing:
                raise ValueError(f"Missing columns for insert into {table_name}: {missing}")

            df2 = df[insert_cols].copy()
            df2 = df2.where(~pd.isna(df2), None)
            values = df2.values.tolist()

            placeholders = ",".join(["?"] * len(insert_cols))
            cols_sql = ",".join(insert_cols)
            sql = f"INSERT OR IGNORE INTO {table_name} ({cols_sql}) VALUES ({placeholders})"

            conn.executemany(sql, values)
            conn.commit()
        finally:
            conn.close()