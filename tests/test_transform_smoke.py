import pandas as pd

from src.transforms.patiotuerca import PatiotuercaTransformer, to_int


def test_to_int_basic() -> None:
    assert to_int("16.500") == 16500
    assert to_int(None) is None


def test_transform_empty_dataframe() -> None:
    df = pd.DataFrame()
    out = PatiotuercaTransformer().transform(df)
    assert out.empty


def test_transform_single_row() -> None:
    df = pd.DataFrame(
        [
            {
                "scraped_at": "2026-01-01T00:00:00+00:00",
                "source_url": "https://example.com",
                "title": "Test car",
                "image": "//img",
                "full_price": None,
                "price_raw": "$10.000",
                "year_raw": "2020",
                "plan": "GOLD",
                "brand": "X",
                "model": "Y",
                "location": "Quito",
                "type": "Sedan",
                "mileage_raw": "50.000",
            }
        ]
    )
    out = PatiotuercaTransformer().transform(df)
    assert len(out) == 1
    assert out.iloc[0]["year"] == 2020
    assert out.iloc[0]["price"] == "10.000"
    assert "item_hash" in out.columns
