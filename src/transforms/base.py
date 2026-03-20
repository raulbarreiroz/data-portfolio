from typing import Protocol
import pandas as pd

class Transformer(Protocol):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame: ...