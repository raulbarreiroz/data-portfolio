import requests
import re
from datetime import datetime, timezone
from typing import Any, Tuple, List, Dict
from bs4 import BeautifulSoup
from src.connectors.patiotuerca.config import BASE_URL, START_PATH
from src.connectors.base import SourceConnector  

def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    out: str = re.sub(r"\s+", " ", str(value)).strip()
    return out or None

def extract_cards_from_html(
    html: str, source_url: str, scraped_at: str, current_rows_length: int = 0
) -> Tuple[List[Dict[str, Any]], str | None]:
    soup = BeautifulSoup(html, "html.parser")
    cards_html = soup.find_all("div", class_='vehicle-card-listing-item')
    rows: List[Dict[str, Any]] = []

    print('===============================================')
    for idx, el in enumerate(cards_html):        
        a_link = el.find("a", class_="link")
        img = el.find("img", class_="photo")
        full_price_el = el.find("div", class_="full-price")
        price_el = el.find("strong", class_="price-text")
        year_el = el.find("div", class_="year")
        plan_el = el.find("div", class_="plan")
        model_el = el.find("div", class_="model")
        location_el = el.find("div", class_="location")
        card: dict[str, Any] = {
            "scraped_at": scraped_at,
            "source_url": source_url,
            "index_in_page": idx,
            "title": clean_text(a_link.get("title") if a_link else None),
            "image": clean_text(img.get("src") if img else None),
            "full_price": clean_text(full_price_el.text if full_price_el else None),
            "price_raw": clean_text(price_el.text if price_el else None),
            "year_raw": clean_text(year_el.text if year_el else None),
            "plan": clean_text(plan_el.text if plan_el else None),
            "model": clean_text(model_el.text if model_el else None),
            "location": clean_text(location_el.text if location_el else None),
            "type": None,
            "mileage_raw": None,
        }

        brand = None
        brand_model = el.find("div", class_="brand-model")
        if brand_model:
            h2 = brand_model.find("h2")
            if h2:
                brand = h2.find(string=True, recursive=False)
        card["brand"] = clean_text(brand)
        extras = el.find("div", class_="extras")
        if extras:
            extras_children = [x for x in extras.children if getattr(x, "text", None)]
            if len(extras_children) > 1:
                card["type"] = clean_text(extras_children[1].text.replace(",", ""))
            if len(extras_children) > 3:
                card["mileage_raw"] = clean_text(
                    extras_children[3].text.replace("Kms", "").replace(",", "")
                )
        rows.append(card)
        print(f'Extracting element number { current_rows_length + len(rows)}')

    next_link = soup.find("a", attrs={'rel': 'next'})
    next_path: str | None = next_link.get('href') if next_link else None
    return rows, next_path

class PatiotuercaConnector(SourceConnector):
    # Connector implementation following generic contract
    def extract(self, max_urls_counter: int, max_data_length: int, timeout: int) -> List[Dict[str, Any]]:
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            }
        )
        all_rows: List[Dict[str, Any]] = []
        current_path: str | None = START_PATH
        page_count = 0

        while current_path and page_count < max_urls_counter and len(all_rows) < max_data_length:
            page_count += 1
            current_url = BASE_URL + current_path
            print(f"[{page_count}/{max_urls_counter}] {current_url}")

            resp = session.get(current_url, timeout=timeout)
            resp.raise_for_status()

            scraped_at = datetime.now(timezone.utc).isoformat()
            rows, next_path = extract_cards_from_html(resp.text, current_url, scraped_at, len(all_rows))

            remaining = max_data_length -len(all_rows)
            print('Elements that will be added to the list: ', len(rows[:remaining]))
            all_rows.extend(rows[:remaining])
            current_path = next_path

        return all_rows