from typing import Protocol

import pandas as pd


class Loader(Protocol):
    def load(self, df: pd.DataFrame, db_url: str, table_name: str) -> None: ...
