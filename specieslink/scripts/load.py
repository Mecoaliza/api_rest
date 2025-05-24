import pandas as pd
from pymongo import MongoClient

def load_data_to_mongo(data):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['species']
    colecao = db['especies']
    dados = data.to_dict(orient='records')

    colecao.insert_many(dados)

    print('Importação concluída com sucesso!')