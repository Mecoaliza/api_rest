import requests
import pandas as pd
import os
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from dotenv import load_dotenv
import time

load_dotenv()
TOKEN = os.getenv('TOKEN')


spark = SparkSession.builder.appName("API SP").getOrCreate()

key = TOKEN
PAGES = 5000
url_base = 'https://specieslink.net/ws/1.0/search'


def get_data_from_api(url):
    try:
        response = requests.get(url)
        response.raise_for_status() 
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer requisição: {e}")
        return None


def build_url(offset, limit):
    return f'{url_base}?stateprovince=Alagoas&apikey={key}&offset={offset}&limit={limit}'

def extract_all_data():
    first_url = build_url(0, PAGES)
    first_response = get_data_from_api(first_url)

    if not first_response:
        print("Falha ao obter dados iniciais.")
        return[]
    
    total = first_response.get('numberMatched', 0)
    total_pages = (total // PAGES) + 1

    all_data = []

    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_data_from_api, build_url(i * PAGES, PAGES)) for i in range(total_pages)]

        for future in as_completed(futures):
            data = future.result()
            if data and 'features' in data:
                all_data.extend(data['features'])

    return all_data

def save_raw_data(data, path='../data_raw/all_records.json'):
    with open(path, 'w') as f:
        json.dump(data, f)
