import json
import pandas as pd

path_json = '../data_raw/all_records.json'

# 1. Iniciando a leitura dos dados

def read_json(path_json):
    data = []
    with open(path_json, 'r') as file:
        data = json.load(file)
    return data

# Função para casos que tenha arquivos de origens diferentes
def read_datas(path, type_file):
    dados = []

    if type_file == 'csv':
        dados = read_json(path)
    elif type_file == 'json':
        dados = read_json(path)

    return dados

def get_columns(dados):
    return list(dados[0].keys())

# Transformação dos dados

# 2. Tratando dados

def remove_type_key(dados):
    for record in dados:
        if 'type' in record:
            del record['type']

        if 'geometry' in record and 'type' in record['geometry']:
            del record['geometry']['type']

    return dados


def convert_coordinates_to_string(dados):
    for record in dados:  
        if 'geometry' in record and 'coordinates' in record['geometry']:
            coordinates = record['geometry']['coordinates']
            if isinstance(coordinates, list):
                record['geometry']['coordinates'] = ', '.join(map(str, coordinates))
    return dados


def process_datecollected(dados):
    for record in dados:
        properties = record.get('properties', {})
        day = properties.get('daycollected')
        month = properties.get('monthcollected')
        year = properties.get('yearcollected')
        
        date_parts = [day.zfill(2) if day else '', month.zfill(2) if month else '', year]
        datecollected = '-'.join(part for part in date_parts if part)

        if datecollected:
            properties['datecollected'] = datecollected
            # Remover os campos antigos
            if 'daycollected' in properties:
                del properties['daycollected']
            if 'monthcollected' in properties:
                del properties['monthcollected']
            if 'yearcollected' in properties:
                del properties['yearcollected']
    
    return dados

# Iniciando a leitura dos dados

data = read_datas(path_json, 'json')
#print(data[0]['properties'])

# Resgatando o nome das colunas
collumns_names = get_columns(data)

print(remove_type_key(data)[0])