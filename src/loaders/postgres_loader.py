import pandas as pd
from sqlalchemy import create_engine

class PostgresLoader:
    def load(self, df: pd.DataFrame, db_url: str, table_name: str) -> None:
        if df.empty:
            return
        engine = create_engine(db_url)
        with engine.connect() as conn:
            df.to_sql(name=table_name, con=conn, if_exists="append", index=False)