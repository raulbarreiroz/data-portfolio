import json
import hashlib
import sqlite3
import argparse
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from src.Scraper.Basic import scrap as basic_scrap
from src.Utils.Normalizer import spaces_normalizer
from src.Utils.Waiter import wait


# =================================================
# args params
p = argparse.ArgumentParser()
p.add_argument("max_urls_counter",nargs="?",type=int,default=None) 
p.add_argument("max_data_length",nargs="?",type=int,default=None) 
p.add_argument("--max_urls_counter",dest="max_urls_counter_kw",type=int,default=None)
p.add_argument("--max_data_length",dest="max_data_length_kw",type=int,default=None)
a=p.parse_args()

# params
max_urls_counter=a.max_urls_counter_kw or a.max_urls_counter or 1000
max_data_length=a.max_data_length_kw or a.max_data_length or 1000
# =================================================

iteration_counter = 0
url = 'https://ecuador.patiotuerca.com'
param = '/usados/-/autos'


# 3. SAVE DATA HELPERS
def _card_hash(card: dict) -> str:
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
    payload = json.dumps(stable, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _to_int(x):
    if x is None:
        return None
    if isinstance(x, int):
        return x
    s = str(x).strip().replace(".", "").replace(",", "")
    return int(s) if s.isdigit() else None


def _save_card_to_db(conn: sqlite3.Connection, card: dict, scraped_at: str, inserted_at: str) -> int:
    item_hash = _card_hash(card)
    raw_json = json.dumps(card, ensure_ascii=False, sort_keys=True)

    cur = conn.execute(
        """
        INSERT OR IGNORE INTO patiotuerca_vehicles (
            item_hash, scraped_at, inserted_at, source_url,
            title, image, fullPrice, price, year, plan, brand, model, location, type, mileage,
            raw_json
        ) VALUES (
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?
        );
        """,
        (
            item_hash,
            scraped_at,
            inserted_at,
            card.get("url"),
            card.get("title"),
            card.get("image"),
            card.get("fullPrice"),
            card.get("price"),
            _to_int(card.get("year")),
            card.get("plan"),
            card.get("brand"),
            card.get("model"),
            card.get("location"),
            card.get("type"),
            _to_int(card.get("mileage")),
            raw_json,
        ),
    )
    return cur.rowcount


def _save_failure_to_db(conn: sqlite3.Connection, url: str, el_html: str) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS patiotuerca_failures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT,
            el TEXT
        );
        """
    )
    conn.execute(
        """
        INSERT INTO patiotuerca_failures (url, el)
        VALUES (?, ?);
        """,
        (url, el_html),
    )


db_path = "patiotuerca.sqlite"
conn = sqlite3.connect(db_path)
try:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS patiotuerca_vehicles (
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

    # SEARCH + MANAGE + SAVE PER URL
    total_cards = 0
    cards_for_json = []

    while iteration_counter < max_urls_counter and total_cards < max_data_length:        
        current_url = url + param
        print('--------------------------------')
        print(f'Iteration {iteration_counter + 1} of {max_urls_counter}')
        print(f'URL: {current_url}')
        wait()

        tag = basic_scrap('patiotuerca', current_url, 'html', True)

        # Retry a few times if necessary
        counter = 0
        while tag is None and counter < 3:
            tag = basic_scrap('patiotuerca', current_url, 'html', True)
            counter += 1

        if tag is None and counter == 3:
            print(f'No tag found after {counter} attempts, stopping on URL: {current_url}')
            break

        scraped_at = datetime.now(timezone.utc).isoformat()
        inserted_at = scraped_at

        soup = BeautifulSoup(tag, 'html.parser')
        tags = soup.find_all('div', class_='vehicle-card-listing-item')        
        print(f'Found {len(tags)} vehicle-card-listing-item tags')

        for el in tags:
            if total_cards >= max_data_length:
                break

            print(f'----------------- item: {total_cards} --------------------------')
            
            card = {}
            card['index'] = total_cards
            card['url'] = current_url
            try:
                card['title'] = el.find('a', class_='link').get('title').strip()
            except Exception as e:
                card['title'] = None
                
            try:
                card['image'] = el.find('img', class_='photo').get('src')
            except Exception as e:
                card['image'] = None

            try:            
                card['fullPrice'] = el.find('div', class_='full-price').text.strip()
            except Exception as e:
                card['fullPrice'] = None
            
            try:
                card['price'] = el.find('strong', class_='price-text').text.strip().replace('$', '')
            except Exception as e:
                card['price'] = None

            try:
                card['year'] = el.find('div', class_='year').text.strip()
            except Exception as e:
                card['year'] = None
            
            try:
                card['plan'] = el.find('div', class_='plan').text.strip()
            except Exception as e:
                card['plan'] = None
                        
            try:
                brand_h2 = el.find('div', class_='brand-model').find('h2')
                card['brand'] = brand_h2.find(string=True, recursive=False).strip()
            except Exception as e:
                card['brand'] = None

            try:
                card['model'] = el.find('div', class_='model').text.strip()
            except Exception as e:
                card['model'] = None

            try:
                card['location'] = el.find('div', class_='location').text.strip()
            except Exception as e:
                card['location'] = None

            try:
                extras = el.find('div', class_='extras')
            except Exception as e:
                extras = None
            
            extras_children = list(extras.children)

            for i, extra_el in enumerate(extras_children):
                if i == 1:
                    try:
                        card['type'] = extra_el.text.strip().replace(',', '')
                    except Exception as e:
                        card['type'] = None
                elif i == 3:
                    try:
                        card['mileage'] = extra_el.text.strip().replace('Kms', '').replace('\n', '').replace(' ', '').replace(',', '')
                    except Exception as e:
                        card['mileage'] = None

            card = {key: spaces_normalizer(value) if isinstance(value, str) else value for key, value in card.items() if value}
            card['insertedAt'] = inserted_at

            # Save immediately to DB for this URL/item
            try:
                rows = _save_card_to_db(conn, card, scraped_at=scraped_at, inserted_at=inserted_at)
            except Exception:
                _save_failure_to_db(conn, current_url, str(el))
                continue
            if rows:
                total_cards += 1                
                print(f'Inserted {total_cards} new rows (max_data_length={max_data_length}).')
                cards_for_json.append(card)

        next_link = soup.find('a', attrs={'rel': 'next'})
        if next_link is None:
            print(f'No next link found, stopping on URL: {current_url}')
            break
        param = next_link.get('href')        
        iteration_counter += 1

    # Save all collected cards to JSON (best-effort snapshot)
    with open('patiotuerca.json', 'w', encoding='utf-8') as file:
        json.dump(cards_for_json, file, indent=2)

    conn.commit()
    print(f"Process finished. Inserted {total_cards} new rows (max_data_length={max_data_length}).")
finally:
    conn.close()
