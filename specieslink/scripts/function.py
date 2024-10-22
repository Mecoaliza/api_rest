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

# Iniciando a leitura dos dados

data = read_datas(path_json, 'json')
#print(data[0]['properties'])

# Resgatando o nome das colunas
collumns_names = get_columns(data)
print(collumns_names)