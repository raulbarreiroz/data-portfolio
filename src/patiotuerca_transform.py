import json
import re
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd


def to_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    s: str = re.sub(r"[^\d]", "", str(value))
    return int(s) if s else None


def _to_py_none(x: Any) -> Any:
    if x is None or pd.isna(x):
        return None
    return x


def _card_hash(card: Dict[str, Any]) -> str:
    stable = {
        "title": card.get("title"),
        "year": card.get("year"),
        "price": card.get("price"),
        "brand": card.get("brand"),
        "model": card.get("model"),
        "location": card.get("location"),
        "mileage": card.get("mileage"),
        "type": card.get("type"),
    }
    payload = json.dumps(
        stable,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    out["inserted_at"] = datetime.now(timezone.utc).isoformat()

    out["price"] = out["price_raw"].apply(
        lambda x: None if x is None or pd.isna(x) else str(x).replace("$", "")
    )
    out["year"] = out["year_raw"].map(to_int)
    out["mileage"] = out["mileage_raw"].map(to_int)

    out["fullPrice"] = out["full_price"]

    def build_row(row: pd.Series) -> pd.Series:
        card = {
            "title": _to_py_none(row.get("title")),
            "year": _to_py_none(row.get("year")),
            "price": _to_py_none(row.get("price")),
            "brand": _to_py_none(row.get("brand")),
            "model": _to_py_none(row.get("model")),
            "location": _to_py_none(row.get("location")),
            "mileage": _to_py_none(row.get("mileage")),
            "type": _to_py_none(row.get("type")),
            "plan": _to_py_none(row.get("plan")),
            "image": _to_py_none(row.get("image")),
            "fullPrice": _to_py_none(row.get("fullPrice")),
            "source_url": _to_py_none(row.get("source_url")),
            "scraped_at": _to_py_none(row.get("scraped_at")),
            "insertedAt": _to_py_none(row.get("inserted_at")),
        }

        item_hash = _card_hash(card)
        raw_json = json.dumps(card, ensure_ascii=False, sort_keys=True)

        return pd.Series({"item_hash": item_hash, "raw_json": raw_json})

    extra = out.apply(build_row, axis=1)
    out = pd.concat([out, extra], axis=1)

    final_cols = [
        "item_hash",
        "scraped_at",
        "inserted_at",
        "source_url",
        "title",
        "image",
        "fullPrice",
        "price",
        "year",
        "plan",
        "brand",
        "model",
        "location",
        "type",
        "mileage",
        "raw_json",
    ]
    return out[final_cols]