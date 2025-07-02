import requests
import datetime
import sqlite3
import time
from geopy.geocoders import Nominatim

# Настройки
cities = ['Москва', 'Санкт-Петербург', 'Казань', 'Новосибирск']
queries = ['зубная щетка', 'зубная щетка средней жесткости', 'зубная щетка для брекетов']
max_page = 2
brands = ['president', 'pure by president']
suppliers = ['PRESIDENT']

DB_FILE = 'wb.db'
USER_AGENT = "wb_tracker_app"
GEO_FALLBACK_DEST = 123589415

# Инициализация геолокатора
geolocator = Nominatim(user_agent=USER_AGENT)

def generate_city_params(city):
    """Получает geo-info по городу"""
    location = geolocator.geocode(city)
    if not location:
        print(f"Город {city} не найден.")
        return None

    params = {
        'latitude': location.latitude,
        'longitude': location.longitude,
        'address': city
    }

    try:
        r = requests.get('https://user-geo-data.wildberries.ru/get-geo-info', params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"Не удалось получить geo-info для {city}: {e}")
        return None

def create_db():

    """Создаёт таблицу, если не существует"""

    conn = sqlite3.connect(DB_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS positions (
            name TEXT,
            cpm INTEGER,
            position INTEGER,
            promo_position INTEGER,
            org_position INTEGER,
            type TEXT,
            query TEXT,
            created_at DATETIME,
            is_promo INTEGER,
            city TEXT
        )
    ''')
    conn.commit()
    conn.close()

def save_to_db(data):

    """Сохраняет список товаров в базу данных"""

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO positions
        (name, cpm, position, promo_position, org_position, type, query, created_at, is_promo, city)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', data)
    conn.commit()
    conn.close()

def collect_data():

    """Основной цикл сбора данных"""

    all_results = []

    for city in cities:
        
        geo_info = generate_city_params(city)
        dest_id = geo_info.get('dest', GEO_FALLBACK_DEST) if geo_info else GEO_FALLBACK_DEST

        for query in queries:
            
            for page in range(1, max_page + 1):
                
                params = {
                    'ab_testid': 'no_action',
                    'appType': 1,
                    'curr': 'rub',
                    'dest': dest_id,
                    'hide_dtype': 13,
                    'lang': 'ru',
                    'page': page,
                    'query': query,
                    'resultset': 'catalog',
                    'sort': 'popular',
                    'spp': 30,
                    'suppressSpellcheck': 'false'
                }

                try:
                    response = requests.get('https://search.wb.ru/exactmatch/ru/common/v13/search', params=params, timeout=10)
                    products = response.json()['data']['products']
                except Exception as e:
                    print(f"Ошибка при запросе или парсинге: {e}")
                    continue

                for idx, product in enumerate(products):
                    brand = product.get('brand', '').strip().lower()
                    supplier = product.get('supplier', '').strip()

                    if brand in brands or supplier in suppliers:
                        log_data = product.get('log', {})
                        promo_position = log_data.get('promoPosition')
                        org_position = log_data.get('position', idx + 1)
                        cpm = log_data.get('cpm', 0)
                        tp = log_data.get('tp', '-')
                        is_promo = int(promo_position is not None)
                        effective_position = promo_position if is_promo else org_position

                        all_results.append([
                            product.get('name', ''),
                            cpm,
                            effective_position,
                            promo_position if promo_position is not None else None,
                            org_position,
                            tp,
                            query,
                            datetime.datetime.now(),
                            is_promo,
                            city
                        ])

              
                time.sleep(1)

    return all_results

if __name__ == '__main__':
    create_db()
    data = collect_data()
    if data:
        save_to_db(data)
        print(f"\n Сохранено {len(data)} записей в базу данных.")
    else:
        print("\n Ничего не найдено.")