import json
import os
from typing import Any
from urllib.parse import unquote_plus

import boto3
from botocore.client import BaseClient

s3: BaseClient = boto3.client("s3")
OUTPUT_PREFIX: str = os.getenv("OUTPUT_PREFIX", "patiotuerca/validated/")


def to_int(v: Any) -> int | None:
    if v is None:
        return None
    s: str = "".join(ch for ch in str(v) if ch.isdigit())
    return int(s) if s else None


def dedupe_key(row: dict[str, Any]) -> str:
    h = row.get("item_hash")
    if isinstance(h, str) and h.strip():
        return f"h:{h.strip()}"
    title = (str(row.get("title") or "")).strip().lower()
    brand = (str(row.get("brand") or "")).strip().lower()
    model = (str(row.get("model") or "")).strip().lower()
    image = (str(row.get("image") or "")).strip()
    return (
        f"c:{title}|{brand}|{model}|{row.get('year')}|"
        f"{row.get('price')}|{row.get('mileage')}|{image}"
    )


def apply_duplicate_flags(rows: list[dict[str, Any]]) -> None:
    seen: set[str] = set()
    for row in rows:
        key = dedupe_key(row)
        is_dup = key in seen
        row["is_duplicate"] = is_dup
        seen.add(key)
        if is_dup:
            row["is_valid"] = False


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    records: list[dict[str, Any]] = event.get("Records", [])

    for record in records:
        bucket: str = record["s3"]["bucket"]["name"]
        key: str = unquote_plus(record["s3"]["object"]["key"])

        obj: dict[str, Any] = s3.get_object(Bucket=bucket, Key=key)
        body: bytes = obj["Body"].read()
        data: list[dict[str, Any]] = json.loads(body.decode("utf-8"))

        validated: list[dict[str, Any]] = []
        for row in data:
            row["year"] = to_int(row.get("year"))
            row["mileage"] = to_int(row.get("mileage"))
            row["price"] = to_int(row.get("price"))
            row["fullPrice"] = (
                to_int(row.get("price")) if row.get("fullPrice") is None else row.get("fullPrice")
            )
            row["is_valid"] = row.get("title") is not None and row.get("price") is not None
            validated.append(row)

        apply_duplicate_flags(validated)

        filename: str = key.split("/")[-1]
        out_key: str = f"{OUTPUT_PREFIX}{filename}"

        s3.put_object(
            Bucket=bucket,
            Key=out_key,
            Body=json.dumps(validated, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )

    return {"statusCode": 200}


# AWS default handler name unless you override in function config
lambda_handler = handler
